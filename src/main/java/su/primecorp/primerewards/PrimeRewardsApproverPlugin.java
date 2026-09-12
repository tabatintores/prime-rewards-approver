package su.primecorp.primerewards;

import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;
import su.primecorp.primerewards.config.ConfigFiles;
import su.primecorp.primerewards.config.OrdersConfig;
import su.primecorp.primerewards.core.Dispatcher;
import su.primecorp.primerewards.core.RewardExecutor;
import su.primecorp.primerewards.core.RewardSource;
import su.primecorp.primerewards.mysql.DbPool;
import su.primecorp.primerewards.sources.HotMcVoteRewardSource;
import su.primecorp.primerewards.sources.OrdersRewardSource;
import su.primecorp.primerewards.sources.TelegramSubscriptionRewardSource;
import su.primecorp.primerewards.util.SafeConfig;
import su.primecorp.primerewards.util.TextService;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class PrimeRewardsApproverPlugin extends JavaPlugin {
    private DbPool db;
    private Dispatcher dispatcher;
    private RewardExecutor executor;
    private ExecutorService lifecycleIo;
    private BukkitTask lifecyclePump;
    private CompletableFuture<ConfigFiles> loading;
    private CompletableFuture<DbPool> connecting;
    private SafeConfig mainConfig, tgConfig, votesConfig;
    private OrdersConfig ordersConfig;
    private CommandSender reloadSender;
    private Path configDirectory;
    private Logger log;
    private boolean changing;
    private TextService text = new TextService(Map.of());

    @Override
    public void onEnable() {
        log = getLogger();
        configDirectory = getDataFolder().toPath();
        lifecycleIo = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "PrimeRewards-Lifecycle");
            thread.setDaemon(true);
            return thread;
        });
        // Async-потоки публикуют только результаты; Bukkit проверяет их короткой sync-задачей.
        lifecyclePump = Bukkit.getScheduler().runTaskTimer(this, this::advanceLifecycle, 1L, 1L);
        beginReload(null);
    }

    private void beginReload(CommandSender sender) {
        changing = true;
        reloadSender = sender;
        if (dispatcher != null) dispatcher.stop();
        Dispatcher previousDispatcher = dispatcher;
        DbPool previousDb = db;
        Path directory = configDirectory;
        loading = CompletableFuture.supplyAsync(() -> {
            try {
                if (previousDispatcher != null && !previousDispatcher.awaitStopped(30)) {
                    throw new IllegalStateException("Старые работы ещё не завершены.");
                }
                if (previousDb != null) previousDb.close();
                return ConfigFiles.read(directory);
            } catch (Exception failure) {
                throw new CompletionException(failure);
            }
        }, lifecycleIo);
    }

    private void advanceLifecycle() {
        try {
            if (loading != null && loading.isDone()) {
                ConfigFiles files = loading.join();
                loading = null;
                // Небольшие YAML-конфиги разбираются sync; всё чтение уже завершено async.
                mainConfig = parse(files.main());
                tgConfig = parse(files.telegram());
                votesConfig = parse(files.votes());
                setupLogging(mainConfig);
                Map<String, String> messages = new HashMap<>();
                var section = mainConfig.getConfig().getConfigurationSection("messages");
                if (section != null) {
                    for (String key : section.getKeys(false)) messages.put(key, section.getString(key, ""));
                }
                text = new TextService(messages);
                try {
                    ordersConfig = OrdersConfig.load(mainConfig, log);
                } catch (IllegalArgumentException invalidOrders) {
                    ordersConfig = null;
                    log.severe("Источник заказов отключён: " + invalidOrders.getMessage());
                }
                DbPool.Settings settings = DbPool.Settings.from(mainConfig);
                connecting = CompletableFuture.supplyAsync(() -> new DbPool(settings), lifecycleIo);
            }
            if (connecting != null && connecting.isDone()) {
                db = connecting.join();
                connecting = null;
                startSources();
                changing = false;
                if (reloadSender != null) {
                    if (ordersConfig == null) {
                        text.send(reloadSender, "orders-disabled", "<red>Источник заказов отключён из-за ошибки настройки. Подробности в консоли.");
                    } else {
                        text.send(reloadSender, "reload-success", "<green>Конфигурация перезагружена.");
                    }
                    reloadSender = null;
                }
            }
        } catch (Exception failure) {
            loading = null;
            connecting = null;
            changing = false;
            log.severe("Запуск/перезагрузка не завершены; выдача остановлена. " + Dispatcher.safeFailure(unwrap(failure)));
            if (dispatcher != null) dispatcher.stop();
            if (executor != null) executor.stop();
            if (reloadSender != null) {
                text.send(reloadSender, "reload-failed", "<red>Не удалось перезагрузить конфигурацию. Подробности в консоли.");
                reloadSender = null;
            }
        }
    }

    private void startSources() {
        List<RewardSource> sources = new ArrayList<>();
        if (ordersConfig != null) {
            sources.add(new OrdersRewardSource(db, ordersConfig));
            log.info("Заказы: server=" + ordersConfig.server() + " table=" + ordersConfig.table()
                    + " workerId=" + ordersConfig.workerId());
        }
        if (tgConfig.getConfig().getBoolean("enabled", true)) {
            sources.add(new TelegramSubscriptionRewardSource(db, tgConfig, log));
        }
        if (votesConfig.getConfig().getBoolean("enabled", true)) {
            sources.add(new HotMcVoteRewardSource(db, votesConfig, log));
        }
        executor = new RewardExecutor(this, mainConfig, tgConfig, votesConfig, ordersConfig);
        dispatcher = new Dispatcher(mainConfig, executor, sources, log, db);
        dispatcher.start();
        log.info("PrimeRewardsApprover запущен. Источники: " + sources.stream().map(RewardSource::name).toList());
    }

    private static SafeConfig parse(String content) throws Exception {
        YamlConfiguration yaml = new YamlConfiguration();
        // Без defaults из JAR: новый orders.server не должен незаметно включать legacy-конфиг.
        yaml.loadFromString(content);
        return new SafeConfig(yaml);
    }

    private static Throwable unwrap(Throwable failure) {
        while (failure instanceof CompletionException && failure.getCause() != null) failure = failure.getCause();
        return failure;
    }

    @Override
    public void onDisable() {
        if (lifecyclePump != null) lifecyclePump.cancel();
        if (dispatcher != null) dispatcher.stop();
        if (executor != null) executor.stop();
        Dispatcher previousDispatcher = dispatcher;
        DbPool previousDb = db;
        CompletableFuture<DbPool> pendingPool = connecting;
        Logger logger = log;
        if (lifecycleIo != null) {
            lifecycleIo.execute(() -> {
                try {
                    if (previousDispatcher != null && !previousDispatcher.awaitStopped(30)) {
                        logger.warning("Ожидание БД при остановке истекло. Заказы с резервом требуют ручной сверки.");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    logger.warning("Ожидание остановки прервано; проверьте зарезервированные заказы.");
                } finally {
                    if (previousDb != null) previousDb.close();
                    if (pendingPool != null) {
                        // Задача создания пула находится раньше в той же последовательной очереди.
                        try { pendingPool.join().close(); }
                        catch (CompletionException failure) { logger.warning("Создание ожидающего пула БД завершилось ошибкой."); }
                    }
                }
            });
            lifecycleIo.shutdown();
        }
        if (log != null) log.info("PrimeRewardsApprover остановлен; завершение операций БД выполняется асинхронно.");
    }

    private void setupLogging(SafeConfig cfg) {
        log.setLevel("DEBUG".equalsIgnoreCase(cfg.getString("logging.level", "INFO")) ? Level.FINE : Level.INFO);
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (!command.getName().equalsIgnoreCase("primerewards")) return false;
        if (args.length == 0) {
            text.send(sender, "usage", "<yellow>/primerewards reload <gray>— перезагрузить конфиг; <yellow>/primerewards stats <gray>— метрики.");
            return true;
        }
        switch (args[0].toLowerCase(Locale.ROOT)) {
            case "reload" -> {
                if (!sender.hasPermission("primerewards.reload")) {
                    text.send(sender, "no-permission", "<red>Недостаточно прав.");
                } else if (changing) {
                    text.send(sender, "reload-busy", "<yellow>Загрузка или остановка предыдущих работ ещё выполняется.");
                } else {
                    text.send(sender, "reload-started", "<yellow>Выдача остановлена. Ожидаем завершения работ и загружаем настройки.");
                    beginReload(sender);
                }
            }
            case "stats" -> {
                if (dispatcher == null || changing || !dispatcher.isRunning()) {
                    text.send(sender, "not-ready", "<yellow>Выдача ещё не запущена или перезагружается.");
                } else sender.sendMessage(TextService.parse(dispatcher.dumpStats()));
            }
            default -> text.send(sender, "unknown-command", "<red>Неизвестная подкоманда.");
        }
        return true;
    }
}
