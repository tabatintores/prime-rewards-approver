package su.primecorp.primerewards.config;

import org.bukkit.configuration.file.FileConfiguration;
import su.primecorp.primerewards.util.SafeConfig;

import java.util.Locale;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Неизменяемый контракт источника; имя БД берётся только из JDBC. */
public record OrdersConfig(String server, String table, String workerId, boolean multiserver,
                           String actionsPath, boolean requiresOnline, int queryTimeoutSeconds) {
    private static final Pattern JDBC = Pattern.compile("^jdbc:mysql://[^/]+/([A-Za-z0-9_]{1,64})(?:\\?.*)?$");
    private static final Pattern TABLE = Pattern.compile("([A-Za-z0-9_]{1,64})\\.(orders_anarchy|orders|shop_orders)");

    public static OrdersConfig load(SafeConfig cfg, Logger log) {
        FileConfiguration yaml = cfg.getConfig();
        Matcher jdbc = JDBC.matcher(cfg.getString("mysql.jdbcUrl", ""));
        if (!jdbc.matches()) {
            throw new IllegalArgumentException("Для заказов нужен JDBC URL вида jdbc:mysql://host:port/database с явным именем БД.");
        }
        String database = jdbc.group(1);
        boolean legacy = !yaml.contains("orders", true);
        String server;
        if (legacy) {
            String oldTable = explicitTable(yaml, "table", database);
            server = switch (oldTable) {
                case "orders_anarchy" -> "classic";
                case "orders" -> "hard";
                default -> throw new IllegalArgumentException("Без orders.server разрешены только старые таблицы orders_anarchy/orders; shop_orders требует фильтра режима.");
            };
            log.warning("Старый конфиг заказов: задайте orders.server и orders.workerId; существующий tiers можно оставить. Режим=" + server);
        } else {
            if (!yaml.isConfigurationSection("orders") || !yaml.contains("orders.server", true)) {
                throw new IllegalArgumentException("В секции orders обязательно явно задайте orders.server.");
            }
            String configured = cfg.getString("orders.server", "").trim().toLowerCase(Locale.ROOT);
            server = switch (configured) {
                case "survival", "classic" -> "classic";
                case "duels", "minigames", "hard" -> configured;
                default -> throw new IllegalArgumentException("Пустой или неизвестный orders.server. Допустимы survival (classic), duels, minigames; hard — архивный режим.");
            };
        }
        String tableName = switch (server) {
            case "classic" -> "orders_anarchy";
            case "hard" -> "orders";
            default -> "shop_orders";
        };
        for (String path : new String[]{"table", "orders.table"}) {
            if (yaml.contains(path, true) && !explicitTable(yaml, path, database).equals(tableName)) {
                throw new IllegalArgumentException(path + " конфликтует с orders.server=" + server + "; ожидается " + database + "." + tableName);
            }
        }
        boolean scoped = tableName.equals("shop_orders");
        String worker = cfg.getString("orders.workerId", server + "-" + UUID.randomUUID()).trim();
        if (!worker.matches("[A-Za-z0-9_.:-]{1,128}")) {
            throw new IllegalArgumentException("orders.workerId должен содержать 1–128 символов: латиница, цифры, _, ., :, -.");
        }
        // Один экземпляр обслуживает один режим и использует только свой корневой tiers.
        String path = "tiers";
        boolean online = yaml.getBoolean("orders.requiresOnline", scoped);
        int timeout = Math.max(1, Math.min(60, cfg.getInt("orders.query_timeout_seconds", 10)));
        return new OrdersConfig(server, database + "." + tableName, worker, scoped, path, online, timeout);
    }

    public String sqlTable() {
        String[] parts = table.split("\\.");
        return "`" + parts[0] + "`.`" + parts[1] + "`";
    }

    private static String explicitTable(FileConfiguration yaml, String path, String database) {
        Object value = yaml.get(path);
        Matcher match = TABLE.matcher(value instanceof String text ? text.trim() : "");
        if (!match.matches() || !match.group(1).equals(database)) {
            throw new IllegalArgumentException(path + " должен иметь вид <БД из JDBC>.orders_anarchy/orders/shop_orders. Произвольный SQL и другая БД запрещены.");
        }
        return match.group(2);
    }
}
