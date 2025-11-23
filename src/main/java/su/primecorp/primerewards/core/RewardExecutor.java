package su.primecorp.primerewards.core;

import org.bukkit.Bukkit;
import org.bukkit.command.ConsoleCommandSender;
import org.bukkit.plugin.Plugin;
import su.primecorp.primerewards.util.SafeConfig;
import su.primecorp.primerewards.util.TemplateEngine;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

public final class RewardExecutor {
    private final Plugin plugin;
    private final Logger logger;

    // Раздельные карты действий для разных источников
    private volatile Map<String, List<String>> actionsOrders;
    private volatile Map<String, List<String>> actionsTelegram;

    private final Set<String> executedKeys = ConcurrentHashMap.newKeySet();
    private static final long COMMAND_TIMEOUT_SECONDS = 15;

    public RewardExecutor(Plugin plugin, SafeConfig ordersCfg, SafeConfig tgCfg) {
        this.plugin = plugin;
        this.logger = plugin.getLogger();
        this.actionsOrders = loadActions(ordersCfg);
        this.actionsTelegram = loadActions(tgCfg);
    }

    public void reload(SafeConfig ordersCfg, SafeConfig tgCfg) {
        this.actionsOrders = loadActions(ordersCfg);
        this.actionsTelegram = loadActions(tgCfg);
        logger.info("RewardExecutor reloaded actions: orders=" + actionsOrders.keySet() + " telegram=" + actionsTelegram.keySet());
    }

    private Map<String, List<String>> loadActions(SafeConfig cfg) {
        Map<String, List<String>> map = new HashMap<>();
        if (cfg == null || cfg.getConfig() == null || !cfg.getConfig().isConfigurationSection("tiers")) return map;
        for (String tier : cfg.getConfig().getConfigurationSection("tiers").getKeys(false)) {
            List<String> actions = cfg.getStringList("tiers." + tier);
            map.put(tier.toLowerCase(Locale.ROOT), List.copyOf(actions));
        }
        return map;
    }

    public void execute(RewardItem item, String sourceName) throws Exception {
        Map<String, List<String>> sourceMap =
                "telegram".equalsIgnoreCase(sourceName) ? actionsTelegram : actionsOrders;

        String tierKey = item.tier == null ? "" : item.tier.toLowerCase(Locale.ROOT);
        List<String> actions = sourceMap.get(tierKey);
        if (actions == null || actions.isEmpty()) {
            throw new IllegalStateException("No actions configured for source=" + sourceName + " tier=" + tierKey);
        }

        Map<String, String> ctx = new HashMap<>();
        ctx.put("id", String.valueOf(item.id));
        ctx.put("order_id", item.orderId);
        ctx.put("nickname", item.nickname);
        ctx.put("tier", item.tier);
        ctx.put("amount", String.valueOf(item.amount));
        ctx.put("currency", item.currency == null ? "" : item.currency);
        if (item.attrs != null) item.attrs.forEach((k,v) -> ctx.putIfAbsent(k, v == null ? "" : String.valueOf(v)));

        String idempotencyKey = sourceName + "#" + item.id;
        if (!executedKeys.add(idempotencyKey)) {
            logger.fine("Skip duplicate execute in-process: " + idempotencyKey);
            return;
        }

        ConsoleCommandSender console = Bukkit.getServer().getConsoleSender();
        for (String raw : actions) {
            String cmd = TemplateEngine.apply(raw, ctx);
            boolean ok = runOnMainThread(() -> Bukkit.dispatchCommand(console, cmd));
            if (!ok) throw new RuntimeException("Command failed to dispatch: " + cmd);
        }
    }

    private <T> T runOnMainThread(Callable<T> task) throws Exception {
        if (Bukkit.isPrimaryThread()) return task.call();
        try {
            return Bukkit.getScheduler().callSyncMethod(plugin, task).get(COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException te) {
            throw new RuntimeException("Command execution timeout on main thread", te);
        }
    }
}
