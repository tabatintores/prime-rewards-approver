package su.primecorp.primerewards.core;

import org.bukkit.Bukkit;
import org.bukkit.command.ConsoleCommandSender;
import org.bukkit.plugin.Plugin;
import su.primecorp.primerewards.util.SafeConfig;
import su.primecorp.primerewards.util.TemplateEngine;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

/**
 * Выполняет команды по tier на ГЛАВНОМ потоке.
 * Доп. идемпотентность в процессе (idempotencyKey) — защита от дублей.
 */
public final class RewardExecutor {
    private final Plugin plugin;
    private final Logger logger;
    private volatile Map<String, List<String>> tierActions;

    // простая защита от дублей в рамках одного процесса
    private final Set<String> executedKeys = ConcurrentHashMap.newKeySet();

    // таймаут ожидания выполнения одной команды на главном потоке
    private static final long COMMAND_TIMEOUT_SECONDS = 15;

    public RewardExecutor(Plugin plugin, SafeConfig cfg) {
        this.plugin = plugin;
        this.logger = plugin.getLogger();
        this.tierActions = loadActions(cfg);
    }

    public void reload(SafeConfig cfg) {
        this.tierActions = loadActions(cfg);
        logger.info("RewardExecutor reloaded actions: " + tierActions.keySet());
    }

    private Map<String, List<String>> loadActions(SafeConfig cfg) {
        Map<String, List<String>> map = new HashMap<>();
        if (!cfg.getConfig().isConfigurationSection("tiers")) return map;
        for (String tier : cfg.getConfig().getConfigurationSection("tiers").getKeys(false)) {
            List<String> actions = cfg.getStringList("tiers." + tier);
            map.put(tier.toLowerCase(Locale.ROOT), List.copyOf(actions));
        }
        return map;
    }

    public void execute(RewardItem item) throws Exception {
        String tierKey = item.tier == null ? "" : item.tier.toLowerCase(Locale.ROOT);
        List<String> actions = tierActions.get(tierKey);
        if (actions == null || actions.isEmpty()) {
            throw new IllegalStateException("No actions configured for tier=" + tierKey);
        }

        // формируем плейсхолдеры
        Map<String, String> ctx = new HashMap<>();
        ctx.put("id", String.valueOf(item.id));
        ctx.put("order_id", item.orderId);
        ctx.put("nickname", item.nickname);
        ctx.put("tier", item.tier);
        ctx.put("amount", String.valueOf(item.amount));
        ctx.put("currency", item.currency == null ? "" : item.currency);
        if (item.attrs != null) {
            for (Map.Entry<String, Object> e : item.attrs.entrySet()) {
                ctx.putIfAbsent(e.getKey(), e.getValue() == null ? "" : String.valueOf(e.getValue()));
            }
        }

        String idempotencyKey = "orders#" + item.id;
        if (!executedKeys.add(idempotencyKey)) {
            logger.fine("Skip duplicate execute in-process: " + idempotencyKey);
            return;
        }

        ConsoleCommandSender console = Bukkit.getServer().getConsoleSender();

        for (String raw : actions) {
            String cmd = TemplateEngine.apply(raw, ctx);
            boolean ok = runOnMainThread(() -> Bukkit.dispatchCommand(console, cmd));
            if (!ok) {
                throw new RuntimeException("Command failed to dispatch: " + cmd);
            }
        }
    }

    /**
     * Гарантирует выполнение callable на главном потоке.
     * Если уже на главном — выполняем прямо тут.
     * Иначе — отправляем задачу через callSyncMethod и ждём c таймаутом.
     */
    private <T> T runOnMainThread(Callable<T> task) throws Exception {
        if (Bukkit.isPrimaryThread()) {
            return task.call();
        }
        try {
            return Bukkit.getScheduler()
                    .callSyncMethod(plugin, task)
                    .get(COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException te) {
            throw new RuntimeException("Command execution timeout on main thread", te);
        }
    }
}
