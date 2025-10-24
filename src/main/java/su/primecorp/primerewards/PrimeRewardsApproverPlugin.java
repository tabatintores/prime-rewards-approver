package su.primecorp.primerewards;

import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.JavaPlugin;
import su.primecorp.primerewards.core.Dispatcher;
import su.primecorp.primerewards.core.RewardExecutor;
import su.primecorp.primerewards.core.RewardSource;
import su.primecorp.primerewards.mysql.DbPool;
import su.primecorp.primerewards.sources.OrdersRewardSource;
import su.primecorp.primerewards.util.SafeConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public final class PrimeRewardsApproverPlugin extends JavaPlugin {

    private DbPool db;
    private Dispatcher dispatcher;
    private RewardExecutor executor;
    private final AtomicBoolean started = new AtomicBoolean(false);

    @Override
    public void onEnable() {
        saveDefaultConfig();
        SafeConfig cfg = new SafeConfig(getConfig());
        setupLogging(cfg);

        try {
            this.db = new DbPool(cfg);
        } catch (Exception e) {
            getLogger().log(Level.SEVERE, "Failed to init DB pool", e);
            Bukkit.getPluginManager().disablePlugin(this);
            return;
        }

        this.executor = new RewardExecutor(this, cfg);
        List<RewardSource> sources = new ArrayList<>();
        sources.add(new OrdersRewardSource(db, cfg, getLogger()));

        // ВАЖНО: передаём DbPool явно
        this.dispatcher = new Dispatcher(this, cfg, executor, sources, getLogger(), db);
        this.dispatcher.start();
        started.set(true);

        getLogger().info("PrimeRewardsApprover enabled.");
    }

    @Override
    public void onDisable() {
        if (started.compareAndSet(true, false)) {
            getLogger().info("Stopping dispatcher (safe shutdown)...");
            if (dispatcher != null) dispatcher.stopAndWait();
            if (db != null) db.close();
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
                SafeConfig cfg = new SafeConfig(getConfig());
                executor.reload(cfg);
                dispatcher.reload(cfg);
                sender.sendMessage("§aКонфиг перезагружен.");
            }
            case "stats" -> sender.sendMessage(dispatcher.dumpStats());
            default -> sender.sendMessage("§cНеизвестная подкоманда.");
        }
        return true;
    }
}
