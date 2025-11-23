package su.primecorp.primerewards;

import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import su.primecorp.primerewards.core.Dispatcher;
import su.primecorp.primerewards.core.RewardExecutor;
import su.primecorp.primerewards.core.RewardSource;
import su.primecorp.primerewards.mysql.DbPool;
import su.primecorp.primerewards.sources.OrdersRewardSource;
import su.primecorp.primerewards.sources.TelegramSubscriptionRewardSource;
import su.primecorp.primerewards.util.SafeConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public final class PrimeRewardsApproverPlugin extends JavaPlugin {

    private DbPool dbOrders;
    private DbPool dbTelegram;
    private Dispatcher dispatcher;
    private RewardExecutor executor;
    private FileConfiguration tgConfig;
    private final AtomicBoolean started = new AtomicBoolean(false);

    @Override
    public void onEnable() {
        saveDefaultConfig();
        // создаём tg_rewards.yml при первом запуске
        saveResource("tg_rewards.yml", false);
        this.tgConfig = YamlConfiguration.loadConfiguration(new File(getDataFolder(), "tg_rewards.yml"));

        SafeConfig cfg = new SafeConfig(getConfig());
        SafeConfig tgCfg = new SafeConfig(tgConfig);
        setupLogging(cfg);

        try {
            this.dbOrders = new DbPool(cfg);
        } catch (Exception e) {
            getLogger().log(Level.SEVERE, "Failed to init ORDERS DB pool", e);
            Bukkit.getPluginManager().disablePlugin(this);
            return;
        }

        boolean tgEnabled = tgCfg.getConfig().getBoolean("enabled", true);
        if (tgEnabled) {
            try {
                this.dbTelegram = new DbPool(new SafeConfig(tgConfig));
            } catch (Exception e) {
                getLogger().log(Level.SEVERE, "Failed to init TELEGRAM DB pool", e);
                // телегу можно отключить, чтобы не падать всем плагином
                this.dbTelegram = null;
            }
        }

        this.executor = new RewardExecutor(this, cfg, tgCfg);

        List<RewardSource> sources = new ArrayList<>();
        sources.add(new OrdersRewardSource(dbOrders, cfg, getLogger()));
        if (tgEnabled && dbTelegram != null) {
            sources.add(new TelegramSubscriptionRewardSource(dbTelegram, tgCfg, getLogger()));
        }

        this.dispatcher = new Dispatcher(this, cfg, executor, sources, getLogger(), dbOrders /* не используется конкретным источником, но передаём */);
        this.dispatcher.start();
        started.set(true);

        getLogger().info("PrimeRewardsApprover enabled. Sources: orders"
                + ((tgEnabled && dbTelegram != null) ? ", telegram" : ""));
    }

    @Override
    public void onDisable() {
        if (started.compareAndSet(true, false)) {
            getLogger().info("Stopping dispatcher (safe shutdown)...");
            if (dispatcher != null) dispatcher.stopAndWait();
            if (dbOrders != null) dbOrders.close();
            if (dbTelegram != null) dbTelegram.close();
        }
        getLogger().info("PrimeRewardsApprover disabled.");
    }

    private void setupLogging(SafeConfig cfg) {
        String level = cfg.getString("logging.level", "INFO").toUpperCase();
        Level l = switch (level) {
            case "DEBUG" -> Level.FINE;
            case "INFO" -> Level.INFO;
            default -> Level.INFO;
        };
        getLogger().setLevel(l);
    }

    @Override
    public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
        if (!cmd.getName().equalsIgnoreCase("primerewards")) return false;
        if (args.length == 0) {
            sender.sendMessage("§e/primerewards reload §7— перезагрузить конфиг");
            sender.sendMessage("§e/primerewards stats  §7— показать метрики");
            return true;
        }
        switch (args[0].toLowerCase()) {
            case "reload" -> {
                if (!sender.hasPermission("primerewards.reload")) {
                    sender.sendMessage("§cНедостаточно прав.");
                    return true;
                }
                reloadConfig();
                this.tgConfig = YamlConfiguration.loadConfiguration(new File(getDataFolder(), "tg_rewards.yml"));
                SafeConfig cfg = new SafeConfig(getConfig());
                SafeConfig tgCfg = new SafeConfig(tgConfig);
                executor.reload(cfg, tgCfg);
                dispatcher.reload(cfg);
                sender.sendMessage("§aКонфиг перезагружен.");
            }
            case "stats" -> sender.sendMessage(dispatcher.dumpStats());
            default -> sender.sendMessage("§cНеизвестная подкоманда.");
        }
        return true;
    }
}
