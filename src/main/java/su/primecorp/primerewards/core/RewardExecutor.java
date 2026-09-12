package su.primecorp.primerewards.core;

import org.bukkit.Bukkit;
import org.bukkit.plugin.Plugin;
import org.bukkit.scheduler.BukkitTask;
import su.primecorp.primerewards.config.OrdersConfig;
import su.primecorp.primerewards.util.SafeConfig;
import su.primecorp.primerewards.util.TemplateEngine;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/** Конфигурация неизменяема. Только drain/stop вызываются в основном потоке. */
public final class RewardExecutor {
    private final Map<String, Map<String, List<String>>> actionsBySource;
    private final OrdersConfig orders;
    private final BlockingQueue<MainRequest<?>> mainQueue = new ArrayBlockingQueue<>(64);
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final BukkitTask pump;
    private final int requestsPerTick;
    private final long timeoutMs;

    public record PreparedReward(List<String> commands, String nickname, boolean requiresOnline, String server) {}

    public static final class DeliveryException extends Exception {
        public DeliveryException(String message) { super(message); }
    }

    public RewardExecutor(Plugin plugin, SafeConfig ordersCfg, SafeConfig tgCfg, SafeConfig votesCfg,
                          OrdersConfig orders) {
        this.orders = orders;
        this.actionsBySource = Map.of(
                "orders", orders == null ? Map.of() : loadActions(ordersCfg, "tiers"),
                "telegram", loadActions(tgCfg, "tiers"),
                "votes", loadActions(votesCfg, "tiers"));
        this.requestsPerTick = Math.max(1, Math.min(8, ordersCfg.getInt("execution.requests_per_tick", 2)));
        this.timeoutMs = Math.max(1000L, Math.min(60000L, ordersCfg.getLong("execution.command_timeout_ms", 15000L)));
        this.pump = Bukkit.getScheduler().runTaskTimer(plugin, this::drain, 1L, 1L);
    }

    private Map<String, List<String>> loadActions(SafeConfig cfg, String path) {
        Map<String, List<String>> map = new HashMap<>();
        var section = cfg.getConfig().getConfigurationSection(path);
        if (section != null) {
            for (String tier : section.getKeys(false)) {
                map.put(tier.toLowerCase(Locale.ROOT), List.copyOf(section.getStringList(tier)));
            }
        }
        return Map.copyOf(map);
    }

    /** Подготовка и онлайн-проверка выполняются до DB claim. Никаких команд здесь нет. */
    public PreparedReward prepare(RewardItem item, String sourceName) throws Exception {
        ensureActive();
        boolean order = "orders".equals(sourceName);
        if (order && orders == null) throw new DeliveryException("Источник заказов отключён.");
        String tierKey = item.tier == null ? "" : item.tier.toLowerCase(Locale.ROOT);
        List<String> actions = actionsBySource.getOrDefault(sourceName, Map.of()).get(tierKey);
        List<String> commands = prepareCommands(item, actions, order ? orders : null);
        PreparedReward prepared = new PreparedReward(commands, item.nickname, order && orders.requiresOnline(),
                order ? item.getAttrAsString("server") : null);
        if (prepared.requiresOnline()) {
            onMain(allowed -> {
                checkAllowed(allowed);
                checkOnline(prepared);
                return null;
            });
        }
        return prepared;
    }

    /** Чистая подготовка команд по снимку; null orderConfig используется для голосов и Telegram. */
    public static List<String> prepareCommands(RewardItem item, List<String> actions, OrdersConfig orderConfig)
            throws DeliveryException {
        if (orderConfig != null) {
            if (!orderConfig.server().equals(item.getAttrAsString("server"))) {
                throw new DeliveryException("Режим заказа не совпадает с режимом источника.");
            }
            if (item.nickname == null || !item.nickname.matches("[A-Za-z0-9_]{3,16}")) {
                throw new DeliveryException("Недопустимый ник в заказе.");
            }
            if (item.orderId == null || !item.orderId.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")) {
                throw new DeliveryException("Некорректный UUID заказа.");
            }
            Object quantity = item.attrs.get("quantity");
            if (quantity != null && (!(quantity instanceof Number count) || count.longValue() < 1)) {
                throw new DeliveryException("Некорректное число пакетов в заказе.");
            }
        }
        if (actions == null || actions.isEmpty()) {
            String safeTier = (item.tier == null ? "" : item.tier.toLowerCase(Locale.ROOT)).replaceAll("[^a-z0-9_-]", "?");
            throw new DeliveryException("Нет команд для tier=" + safeTier.substring(0, Math.min(64, safeTier.length())) + "; заказ не выдан.");
        }
        if (actions.size() > 16) throw new DeliveryException("На одну награду допускается не более 16 команд.");

        Map<String, String> ctx = new HashMap<>();
        ctx.put("id", String.valueOf(item.id));
        ctx.put("order_id", item.orderId);
        ctx.put("nickname", item.nickname);
        ctx.put("tier", item.tier);
        ctx.put("amount", String.valueOf(item.amount));
        ctx.put("currency", item.currency == null ? "" : item.currency);
        item.attrs.forEach((key, value) -> ctx.putIfAbsent(key,
                value == null ? (orderConfig == null ? "" : null) : String.valueOf(value)));

        List<String> commands = new ArrayList<>();
        for (String raw : actions) {
            if (raw.isBlank() || raw.indexOf('\n') >= 0 || raw.indexOf('\r') >= 0) {
                throw new DeliveryException("Пустая или многострочная команда в настройках.");
            }
            if (orderConfig != null && raw.contains("${grant_qty}") && Long.parseLong(item.getAttrAsString("grant_qty")) <= 0) {
                throw new DeliveryException("Для команды валюты или ключей нужен положительный сохранённый grant_qty.");
            }
            String command;
            try {
                command = (orderConfig != null ? TemplateEngine.applyStrict(raw, ctx) : TemplateEngine.apply(raw, ctx)).trim();
            } catch (IllegalArgumentException invalidTemplate) {
                throw new DeliveryException(invalidTemplate.getMessage());
            }
            if (command.startsWith("/")) command = command.substring(1);
            if (command.isBlank()) throw new DeliveryException("Пустая команда после подстановки.");
            commands.add(command);
        }
        return List.copyOf(commands);
    }

    /** Вызывается воркером только после подтверждённого commit резервирования. */
    public void execute(PreparedReward reward) throws Exception {
        onMain(allowed -> {
            if (reward.server() != null && (orders == null || !orders.server().equals(reward.server()))) {
                throw new DeliveryException("Режим подготовленного заказа не совпадает с исполнителем.");
            }
            for (String command : reward.commands()) {
                checkAllowed(allowed);
                checkOnline(reward);
                if (!Bukkit.dispatchCommand(Bukkit.getConsoleSender(), command)) {
                    throw new DeliveryException("Команда вернула false; результат требует сверки.");
                }
            }
            return null;
        });
    }

    private static void checkOnline(PreparedReward reward) throws DeliveryException {
        if (reward.requiresOnline()) {
            var player = Bukkit.getPlayerExact(reward.nickname());
            if (player == null || !player.isOnline()) {
                throw new DeliveryException("Игрок отсутствует на этом сервере; выдача отложена.");
            }
        }
    }

    private void ensureActive() throws DeliveryException {
        if (!accepting.get()) throw new DeliveryException("Исполнитель остановлен.");
    }

    private static void checkAllowed(BooleanSupplier allowed) throws DeliveryException {
        if (!allowed.getAsBoolean()) throw new DeliveryException("Выдача остановлена или истекло время ожидания.");
    }

    @FunctionalInterface
    private interface MainAction<T> {
        T run(BooleanSupplier allowed) throws Exception;
    }

    private final class MainRequest<T> {
        private final CompletableFuture<T> result = new CompletableFuture<>();
        private final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        private final MainAction<T> action;

        private MainRequest(MainAction<T> action) { this.action = action; }

        private boolean allowed() {
            return accepting.get() && !result.isDone() && System.nanoTime() < deadline;
        }

        private void run() {
            if (!allowed()) {
                result.completeExceptionally(new DeliveryException("Запрос в основной поток отменён до запуска."));
                return;
            }
            try {
                result.complete(action.run(this::allowed));
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            }
        }
    }

    private <T> T onMain(MainAction<T> action) throws Exception {
        ensureActive();
        MainRequest<T> request = new MainRequest<>(action);
        if (!mainQueue.offer(request)) throw new DeliveryException("Очередь выдачи заполнена.");
        // Закрывает гонку между stop() и добавлением запроса воркером.
        if (!accepting.get()) request.result.cancel(false);
        try {
            return request.result.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            request.result.cancel(false);
            throw new DeliveryException("Таймаут команды; результат требует сверки.");
        } catch (InterruptedException interrupted) {
            request.result.cancel(false);
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (ExecutionException failure) {
            if (failure.getCause() instanceof DeliveryException known) throw known;
            // Не выводим команды, пароли, JDBC URL или сообщения сторонних плагинов.
            throw new DeliveryException("Ошибка обработчика команды; результат требует сверки.");
        } finally {
            mainQueue.remove(request);
        }
    }

    private void drain() {
        for (int i = 0; i < requestsPerTick; i++) {
            MainRequest<?> request = mainQueue.poll();
            if (request == null) return;
            request.run();
        }
    }

    /** Основной поток: запретить новые команды и отменить ещё не начатые. */
    public void stop() {
        accepting.set(false);
        pump.cancel();
        MainRequest<?> request;
        while ((request = mainQueue.poll()) != null) request.result.cancel(false);
    }
}
