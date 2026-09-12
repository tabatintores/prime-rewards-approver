package su.primecorp.primerewards;

import org.bukkit.configuration.file.YamlConfiguration;
import su.primecorp.primerewards.config.OrdersConfig;
import su.primecorp.primerewards.core.RewardItem;
import su.primecorp.primerewards.mysql.DbPool;
import su.primecorp.primerewards.sources.OrdersRewardSource;
import su.primecorp.primerewards.util.SafeConfig;
import su.primecorp.primerewards.util.TemplateEngine;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Только ручной запуск checkOrdersMysql с новой пустой локальной тестовой БД.
 * Проверяет настоящий InnoDB и PreparedStatement, не подменяя SQL имитацией.
 */
public final class OrdersMysqlChecks {
    private static final Logger LOG = Logger.getLogger("OrdersMysqlChecks");

    public static void main(String[] args) throws Exception {
        String url = System.getenv("PRIMEREWARDS_TEST_JDBC");
        if (url == null || !url.matches("jdbc:mysql://(?:localhost|127\\.0\\.0\\.1)(?::[0-9]+)?/primerewards_test_[a-zA-Z0-9_]+(?:\\?.*)?")) {
            throw new IllegalArgumentException("Нужна локальная PRIMEREWARDS_TEST_JDBC с БД primerewards_test_<имя>. Production запрещён.");
        }
        SafeConfig base = config(url, "minigames", null);
        base.getConfig().set("mysql.username", System.getenv("PRIMEREWARDS_TEST_USER"));
        base.getConfig().set("mysql.password", System.getenv("PRIMEREWARDS_TEST_PASSWORD"));
        checkConfig(url);
        try (DbPool db = new DbPool(DbPool.Settings.from(base))) {
            createFixtures(db);
            checkSources(db, url);
        }
        System.out.println("Проверки конфигурации, SQL-фильтров, конкуренции и владельца резерва пройдены. Тестовые данные оставлены в тестовой БД.");
    }

    private static SafeConfig config(String url, String server, String table) {
        YamlConfiguration yaml = new YamlConfiguration();
        yaml.set("mysql.jdbcUrl", url);
        if (server != null) {
            yaml.set("orders.server", server);
            yaml.set("orders.workerId", "test-" + (server.isBlank() ? "empty" : server));
        }
        if (table != null) yaml.set("table", table);
        return new SafeConfig(yaml);
    }

    private static void checkConfig(String url) {
        String schema = url.substring(url.lastIndexOf('/') + 1).split("\\?")[0];
        check(OrdersConfig.load(config(url, "survival", null), LOG).server().equals("classic"), "Алиас survival");
        check(OrdersConfig.load(config(url, "classic", null), LOG).table().equals(schema + ".orders_anarchy"), "Алиас classic");
        check(OrdersConfig.load(config(url, "hard", null), LOG).table().equals(schema + ".orders"), "Архив hard");
        check(OrdersConfig.load(config(url, null, schema + ".orders"), LOG).actionsPath().equals("tiers"), "Legacy mapping");
        check(OrdersConfig.load(config(url, null, schema + ".orders_anarchy"), LOG).server().equals("classic"), "Legacy Survival");
        check(OrdersConfig.load(config(url, "duels", null), LOG).actionsPath().equals("tiers"), "Mapping экземпляра");
        reject(config(url, "", null));
        reject(config(url, "unknown", null));
        reject(config(url, "duels", schema + ".orders_anarchy"));
        reject(config(url, "survival", schema + ".shop_orders"));
        reject(config(url, "minigames", "other_db.shop_orders"));
        reject(config(url, "minigames", schema + ".shop_orders WHERE 1=1"));
        reject(config(url, null, schema + ".shop_orders"));
        reject(config(url, null, null));
        SafeConfig missing = config(url, null, schema + ".orders");
        missing.getConfig().set("orders.workerId", "missing-server");
        reject(missing);
    }

    private static void reject(SafeConfig config) {
        try {
            OrdersConfig.load(config, LOG);
            throw new AssertionError("Небезопасная конфигурация принята");
        } catch (IllegalArgumentException expected) {
            // Ожидается отказ, без fallback.
        }
    }

    private static void createFixtures(DbPool db) throws Exception {
        try (Connection c = db.getConnection()) {
            // Любая существующая таблица означает отказ: не удаляем и не заменяем данные.
            try (ResultSet tables = c.getMetaData().getTables(c.getCatalog(), null, "%", new String[]{"TABLE", "VIEW"})) {
                check(!tables.next(), "Для теста нужна НОВАЯ ПУСТАЯ база");
            }
            try (Statement s = c.createStatement()) {
                s.setQueryTimeout(10);
                String common = "id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, order_id CHAR(36) NOT NULL UNIQUE,"
                        + " nickname VARCHAR(16) NOT NULL, tier VARCHAR(64) NOT NULL, grant_qty INT UNSIGNED NOT NULL,"
                        + " amount DECIMAL(12,2) NOT NULL, currency CHAR(3) NOT NULL DEFAULT 'RUB',"
                        + " status VARCHAR(16) NOT NULL, paid_at DATETIME NULL, unitpay_id VARCHAR(64) NULL,"
                        + " is_test TINYINT NULL DEFAULT 0, delivered_at DATETIME NULL,"
                        + " delivery_attempts INT NOT NULL DEFAULT 0, delivery_note VARCHAR(255) NULL";
                s.execute("CREATE TABLE orders (" + common + ") ENGINE=InnoDB");
                s.execute("CREATE TABLE orders_anarchy (" + common + ") ENGINE=InnoDB");
                s.execute("CREATE TABLE shop_orders (" + common
                        + ", server VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,"
                        + " quantity INT UNSIGNED NOT NULL DEFAULT 1, product_title VARCHAR(255) NOT NULL,"
                        + " delivery_claim_token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,"
                        + " delivery_claimed_at DATETIME NULL, delivery_worker VARCHAR(128) NULL,"
                        + " KEY ix_delivery(server,status,delivered_at,delivery_claim_token,paid_at,id)) ENGINE=InnoDB");
            }
        }
    }

    private static String insert(DbPool db, String table, String server, String tier, int quantity, int grant,
                                 String status, Integer test, boolean delivered, boolean claimed) throws Exception {
        String id = UUID.randomUUID().toString();
        boolean scoped = table.equals("shop_orders");
        String sql = "INSERT INTO " + table
                + " (order_id,nickname,tier,grant_qty,amount,status,paid_at,is_test,delivered_at"
                + (scoped ? ",server,quantity,product_title,delivery_claim_token,delivery_claimed_at" : "")
                + ") VALUES (?,'SamePlayer',?,?,123.45,?,'2026-01-01 00:00:00',?,?"
                + (scoped ? ",?,?,?,?,?" : "") + ")";
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, tier);
            ps.setInt(3, grant);
            ps.setString(4, status);
            ps.setObject(5, test);
            ps.setTimestamp(6, delivered ? Timestamp.valueOf("2026-01-02 00:00:00") : null);
            if (scoped) {
                ps.setString(7, server);
                ps.setInt(8, quantity);
                ps.setString(9, "Тестовые коины");
                ps.setString(10, claimed ? UUID.randomUUID().toString() : null);
                ps.setTimestamp(11, claimed ? Timestamp.valueOf("2000-01-01 00:00:00") : null);
            }
            check(ps.executeUpdate() == 1, "Создание фикстуры");
        }
        return id;
    }

    private static void checkSources(DbPool db, String url) throws Exception {
        String miniId = insert(db, "shop_orders", "minigames", "coins_1000", 2, 2200, "paid", 0, false, false);
        String miniBigId = insert(db, "shop_orders", "minigames", "coins_5000", 3, 18000, "paid", 0, false, false);
        String duelId = insert(db, "shop_orders", "duels", "coins_1000", 2, 77, "paid", 0, false, false);
        insert(db, "shop_orders", "minigames", "coins_1000", 2, 2200, "created", 0, false, false);
        insert(db, "shop_orders", "minigames", "coins_1000", 2, 2200, "paid", 1, false, false);
        insert(db, "shop_orders", "minigames", "coins_1000", 2, 2200, "paid", null, false, false);
        insert(db, "shop_orders", "minigames", "coins_1000", 2, 2200, "paid", 0, true, false);
        insert(db, "shop_orders", "minigames", "coins_1000", 2, 2200, "paid", 0, false, true);
        insert(db, "shop_orders", "unknown", "coins_1000", 2, 2200, "paid", 0, false, false);
        String classicId = insert(db, "orders_anarchy", null, "coins_1000", 1, 9, "paid", null, false, false);
        insert(db, "orders_anarchy", null, "coins_1000", 1, 9, "paid", 1, false, false);
        String hardId = insert(db, "orders", null, "coins_1000", 1, 5, "paid", 0, false, false);

        OrdersRewardSource mini = source(db, url, "minigames");
        OrdersRewardSource duel = source(db, url, "duels");
        OrdersRewardSource classic = source(db, url, "survival");
        OrdersRewardSource hard = source(db, url, "hard");
        List<RewardItem> miniRows = mini.fetchPending(50);
        check(miniRows.size() == 2 && miniRows.get(0).orderId.equals(miniId) && miniRows.get(1).orderId.equals(miniBigId), "Фильтры MiniGames и ORDER BY id");
        check(mini.fetchPending(1).size() == 1, "LIMIT");
        check(duel.fetchPending(50).size() == 1 && duel.fetchPending(50).getFirst().orderId.equals(duelId), "Изоляция Duels");
        RewardItem classicItem = classic.fetchPending(50).getFirst();
        check(classic.fetchPending(50).size() == 1 && classicItem.orderId.equals(classicId)
                && classicItem.getAttrAsString("server").equals("classic"), "Legacy без новых колонок");
        check(hard.fetchPending(50).getFirst().orderId.equals(hardId), "Изоляция Hard");

        YamlConfiguration defaults;
        try (var resource = Objects.requireNonNull(OrdersMysqlChecks.class.getResourceAsStream("/config.yml"));
             var reader = new InputStreamReader(resource, StandardCharsets.UTF_8)) {
            defaults = YamlConfiguration.loadConfiguration(reader);
        }
        for (RewardItem row : miniRows) {
            Map<String, String> ctx = new HashMap<>();
            row.attrs.forEach((k, v) -> ctx.put(k, String.valueOf(v)));
            ctx.put("nickname", row.nickname);
            List<String> commands = defaults.getStringList("tiers." + row.tier);
            check(commands.size() == 1, "Одна команда MiniGames");
            check(TemplateEngine.applyStrict(commands.getFirst(), ctx).equals("money give SamePlayer " + row.getAttrAsString("grant_qty")),
                    "Готовый grant_qty без умножения quantity");
        }

        RewardItem item = miniRows.getFirst();
        String untouched = snapshot(db, item.id);
        AtomicInteger commandCallbacks = new AtomicInteger();
        AtomicReference<String> owner = new AtomicReference<>();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService competitors = Executors.newFixedThreadPool(2);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                OrdersRewardSource worker = source(db, url, "minigames");
                futures.add(competitors.submit(() -> {
                    start.await();
                    String token = UUID.randomUUID().toString();
                    if (tx(db, c -> worker.claim(c, item, token))) {
                        owner.set(token);
                        commandCallbacks.incrementAndGet(); // Только после commit.
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) future.get(30, TimeUnit.SECONDS);
        } finally {
            competitors.shutdownNow();
        }
        check(commandCallbacks.get() == 1, "Только один победитель claim/commit");
        String token = owner.get();
        String foreignToken = UUID.randomUUID().toString();
        check(!tx(db, c -> mini.markDelivered(c, item, foreignToken)), "Чужой токен не завершает заказ");
        check(!tx(db, c -> mini.markFailed(c, item, foreignToken, "ошибка")), "Чужой токен не меняет ошибку");
        RewardItem wrongOrder = new RewardItem(item.id, UUID.randomUUID().toString(), item.nickname, item.tier, item.amount, item.currency, item.attrs);
        check(!tx(db, c -> mini.markDelivered(c, wrongOrder, token)), "Чужой order_id не завершает заказ");
        check(!tx(db, c -> mini.markFailed(c, wrongOrder, token, "ошибка")), "Чужой order_id не меняет ошибку");
        Map<String, Object> foreignAttrs = new HashMap<>(item.attrs);
        foreignAttrs.put("server", "duels");
        RewardItem foreign = new RewardItem(item.id, item.orderId, item.nickname, item.tier, item.amount, item.currency, foreignAttrs);
        check(!tx(db, c -> mini.markDelivered(c, foreign, token)), "Режим снимка не совпадает");
        check(!tx(db, c -> duel.markDelivered(c, foreign, token)), "SQL server не совпадает");
        check(!tx(db, c -> duel.markFailed(c, foreign, token, "ошибка")), "Ошибка чужого режима не записана");
        check(tx(db, c -> mini.markFailed(c, item, token, "Неопределённый результат; сверить вручную")), "Владелец записывает ошибку");
        check(mini.fetchPending(50).stream().noneMatch(row -> row.orderId.equals(miniId)), "Ошибка сохраняет токен, poll не повторяет выдачу");
        check(!tx(db, c -> mini.claim(c, item, foreignToken)), "Резерв не захватывается повторно");
        check(tx(db, c -> mini.markDelivered(c, item, token)), "Владелец завершает заказ");
        check(!tx(db, c -> mini.markDelivered(c, item, token)), "Повтор результата ничего не меняет");
        check(!tx(db, c -> mini.markFailed(c, item, token, "ошибка")), "Завершённая выдача неизменна");
        check(untouched.equals(snapshot(db, item.id)), "Чужие/тестовые/созданные/резервированные строки не меняются");
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement("SELECT delivery_attempts,delivery_claim_token,grant_qty,quantity,amount,status FROM shop_orders WHERE id=?")) {
            ps.setLong(1, item.id);
            try (ResultSet rs = ps.executeQuery()) {
                check(rs.next() && rs.getInt(1) == 1 && token.equals(rs.getString(2))
                        && rs.getInt(3) == 2200 && rs.getInt(4) == 2
                        && rs.getBigDecimal(5).toPlainString().equals("123.45") && rs.getString(6).equals("paid"), "Попытка учтена один раз; значения заказа сохранены");
            }
        }
        check(tx(db, c -> classic.markDelivered(c, classicItem, null)), "Старая выдача без claim-колонок");
    }

    private static OrdersRewardSource source(DbPool db, String url, String mode) {
        return new OrdersRewardSource(db, OrdersConfig.load(config(url, mode, null), LOG));
    }

    private static String snapshot(DbPool db, long exceptId) throws Exception {
        StringBuilder result = new StringBuilder();
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement("SELECT * FROM shop_orders WHERE id <> ? ORDER BY id")) {
            ps.setLong(1, exceptId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) result.append(rs.getString(col)).append('|');
            }
        }
        return result.toString();
    }

    @FunctionalInterface private interface Operation { boolean run(Connection connection) throws Exception; }

    private static boolean tx(DbPool db, Operation operation) throws Exception {
        try (Connection c = db.getConnection()) {
            c.setAutoCommit(false);
            try {
                boolean changed = operation.run(c);
                if (changed) c.commit(); else c.rollback();
                return changed;
            } catch (Exception failure) {
                c.rollback();
                throw failure;
            }
        }
    }

    private static void check(boolean condition, String name) {
        if (!condition) throw new AssertionError(name);
    }
}
