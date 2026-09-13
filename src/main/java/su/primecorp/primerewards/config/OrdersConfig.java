package su.primecorp.primerewards.config;

import org.bukkit.configuration.file.FileConfiguration;
import su.primecorp.primerewards.util.SafeConfig;

import java.util.Locale;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Неизменяемый контракт источника; все режимы используют shop_orders в БД из JDBC. */
public record OrdersConfig(String server, String table, String workerId,
                           boolean requiresOnline, int queryTimeoutSeconds) {
    private static final Pattern JDBC = Pattern.compile("^jdbc:mysql://[^/]+/([A-Za-z0-9_]{1,64})(?:\\?.*)?$");
    private static final Pattern OLD_TABLE = Pattern.compile("(?:[A-Za-z0-9_]{1,64}\\.)?(orders_anarchy|orders)");

    public static OrdersConfig load(SafeConfig cfg, Logger log) {
        FileConfiguration yaml = cfg.getConfig();
        Matcher jdbc = JDBC.matcher(cfg.getString("mysql.jdbcUrl", ""));
        if (!jdbc.matches()) {
            throw new IllegalArgumentException("Для заказов нужен JDBC URL вида jdbc:mysql://host:port/database с явным именем БД.");
        }
        String database = jdbc.group(1);
        // Старое имя проверяем даже при отсутствии orders.server, чтобы явно объяснить переход.
        for (String path : new String[]{"table", "orders.table"}) {
            if (yaml.contains(path, true)) validateTable(yaml, path, database);
        }
        if (!yaml.isConfigurationSection("orders") || !yaml.contains("orders.server", true)) {
            throw new IllegalArgumentException("Обязательно задайте orders.server — идентификатор режима из shop_orders. Старые таблицы заказов больше не читаются.");
        }
        String configured = cfg.getString("orders.server", "").trim().toLowerCase(Locale.ROOT);
        // Длина соответствует server VARCHAR(32); список названий режимов не ограничивается.
        if (!configured.matches("[a-z0-9_-]{1,32}")) {
            throw new IllegalArgumentException("orders.server должен содержать 1–32 символа: латинские буквы, цифры, _ или -. Укажите идентификатор режима из shop_orders.");
        }
        String server = configured.equals("classic") ? "survival" : configured;
        if (configured.equals("classic")) {
            log.warning("orders.server=classic нормализован в survival. В shop_orders читается только server=survival.");
        }
        String worker = cfg.getString("orders.workerId", server + "-" + UUID.randomUUID()).trim();
        if (!worker.matches("[A-Za-z0-9_.:-]{1,128}")) {
            throw new IllegalArgumentException("orders.workerId должен содержать 1–128 символов: латиница, цифры, _, ., :, -.");
        }
        boolean online = yaml.getBoolean("orders.requiresOnline", true);
        int timeout = Math.max(1, Math.min(60, cfg.getInt("orders.query_timeout_seconds", 10)));
        return new OrdersConfig(server, database + ".shop_orders", worker, online, timeout);
    }

    public String sqlTable() {
        String[] parts = table.split("\\.");
        return "`" + parts[0] + "`.`" + parts[1] + "`";
    }

    private static void validateTable(FileConfiguration yaml, String path, String database) {
        Object value = yaml.get(path);
        String table = value instanceof String text ? text.trim() : "";
        if (OLD_TABLE.matcher(table).matches()) {
            throw new IllegalArgumentException(path + " указывает старую таблицу заказов. Сначала согласованно перенесите историю Survival в shop_orders с сохранением order_id и delivered_at, затем удалите эту настройку и включите новый backend вместе с новым плагином.");
        }
        if (!table.equals("shop_orders") && !table.equals(database + ".shop_orders")) {
            throw new IllegalArgumentException(path + " допускает только shop_orders в БД из JDBC. Другая таблица, схема или произвольный SQL запрещены; ручной выбор таблицы не нужен.");
        }
    }
}
