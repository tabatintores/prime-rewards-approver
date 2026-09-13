package su.primecorp.primerewards;

import org.bukkit.configuration.file.YamlConfiguration;
import su.primecorp.primerewards.config.OrdersConfig;
import su.primecorp.primerewards.core.RewardExecutor;
import su.primecorp.primerewards.core.RewardItem;
import su.primecorp.primerewards.mysql.DbPool;
import su.primecorp.primerewards.sources.OrdersRewardSource;
import su.primecorp.primerewards.util.SafeConfig;

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
 * Проверяет настоящий InnoDB и подготовку команд без запуска Bukkit-команд.
 */
public final class OrdersMysqlChecks {
    private static final Logger LOG = Logger.getLogger("OrdersMysqlChecks");
    private static final List<String> MODES = List.of("survival", "duels", "minigames", "skyblock_2");

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
            OrdersQueueChecks.run(db, url);
        }
        System.out.println("Проверки стандартных и новых режимов, перенесённых заказов, команд и конкуренции пройдены. Фикстуры оставлены в тестовой БД.");
    }

    static SafeConfig config(String url, String server, String table) {
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
        String schema = url.substring("jdbc:mysql://".length()).split("/", 2)[1].split("\\?")[0];
        for (String mode : MODES) {
            OrdersConfig selected = OrdersConfig.load(config(url, mode, null), LOG);
            check(selected.server().equals(mode) && selected.table().equals(schema + ".shop_orders"), "Единая таблица: " + mode);
            check(OrdersConfig.load(config(url, mode, schema + ".shop_orders"), LOG).server().equals(mode), "Совместимое имя таблицы");
            reject(config(url, mode, schema + ".orders_anarchy"));
            reject(config(url, mode, "orders"));
            SafeConfig nestedOld = config(url, mode, null);
            nestedOld.getConfig().set("orders.table", schema + ".orders");
            reject(nestedOld);
        }
        check(OrdersConfig.load(config(url, "classic", null), LOG).server().equals("survival"), "Алиас classic канонизируется");
        reject(config(url, "", null));
        reject(config(url, "   ", null));
        for (String mode : List.of("unknown", "hard", "new-mode", "a", "a".repeat(32))) {
            check(OrdersConfig.load(config(url, mode, null), LOG).server().equals(mode), "Нет списка разрешённых имён: " + mode);
        }
        SafeConfig normalized = config(url, "skyblock_2", null);
        normalized.getConfig().set("orders.server", " SKYBLOCK_2 ");
        check(OrdersConfig.load(normalized, LOG).server().equals("skyblock_2"), "Регистр и внешние пробелы нормализуются");
        for (String invalid : List.of("a".repeat(33), "sky block", "sky.block", "../sky", "sky' OR 1=1", "выживание")) {
            reject(config(url, invalid, null));
        }
        reject(config(url, null, schema + ".orders"));
        reject(config(url, null, schema + ".orders_anarchy"));
        reject(config(url, null, schema + ".shop_orders"));
        reject(config(url, null, null));
        reject(config(url, "minigames", "other_db.shop_orders"));
        reject(config(url, "minigames", schema + ".shop_orders WHERE 1=1"));
        SafeConfig missing = config(url, null, null);
        missing.getConfig().set("orders.workerId", "missing-server");
        reject(missing);
    }

    private static void reject(SafeConfig config) {
        try {
            OrdersConfig.load(config, LOG);
            throw new AssertionError("Небезопасная конфигурация принята");
        } catch (IllegalArgumentException expected) {
            check(!expected.getMessage().isBlank(), "Понятная диагностика настройки");
        }
    }

    private static void createFixtures(DbPool db) throws Exception {
        try (Connection c = db.getConnection()) {
            try (ResultSet tables = c.getMetaData().getTables(c.getCatalog(), null, "%", new String[]{"TABLE", "VIEW"})) {
                check(!tables.next(), "Для теста нужна НОВАЯ ПУСТАЯ база");
            }
            // Создаём только shop_orders. Любой оставшийся запрос к старым таблицам сломает тест.
            try (Statement s = c.createStatement()) {
                s.setQueryTimeout(10);
                s.execute("CREATE TABLE shop_orders ("
                        + " id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,"
                        + " order_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,"
                        + " server VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,"
                        + " nickname VARCHAR(16) NOT NULL, tier VARCHAR(64) NOT NULL,"
                        + " quantity INT UNSIGNED NULL DEFAULT 1, product_title VARCHAR(255) NULL,"
                        + " grant_qty INT UNSIGNED NOT NULL, amount DECIMAL(12,2) NOT NULL,"
                        + " currency CHAR(3) NOT NULL DEFAULT 'RUB', status VARCHAR(16) NOT NULL,"
                        + " paid_at DATETIME NULL, unitpay_id VARCHAR(64) NULL, is_test TINYINT NOT NULL DEFAULT 0,"
                        + " delivered_at DATETIME NULL, delivery_attempts INT NOT NULL DEFAULT 0, delivery_note VARCHAR(255) NULL,"
                        + " delivery_claim_token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,"
                        + " delivery_claimed_at DATETIME NULL, delivery_worker VARCHAR(128) NULL,"
                        + " KEY ix_delivery(server,status,delivered_at,delivery_claim_token,paid_at,id)) ENGINE=InnoDB");
            }
        }
    }

    static String insert(DbPool db, String server, String tier, Integer quantity, int grant,
                                 String status, int test, boolean delivered, boolean claimed) throws Exception {
        String id = UUID.randomUUID().toString();
        String sql = "INSERT INTO shop_orders"
                + " (order_id,nickname,tier,grant_qty,amount,status,paid_at,is_test,delivered_at,"
                + " server,quantity,product_title,delivery_claim_token,delivery_claimed_at)"
                + " VALUES (?,'SamePlayer',?,?,123.45,?,'2026-01-01 00:00:00',?,?,?,?,?,?,?)";
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, tier);
            ps.setInt(3, grant);
            ps.setString(4, status);
            ps.setInt(5, test);
            ps.setTimestamp(6, delivered ? Timestamp.valueOf("2026-01-02 00:00:00") : null);
            ps.setString(7, server);
            ps.setObject(8, quantity, Types.INTEGER);
            ps.setString(9, quantity == null ? null : "Тестовые коины");
            ps.setString(10, claimed ? UUID.randomUUID().toString() : null);
            ps.setTimestamp(11, claimed ? Timestamp.valueOf("2000-01-01 00:00:00") : null);
            check(ps.executeUpdate() == 1, "Создание фикстуры");
        }
        return id;
    }

    private static void checkSources(DbPool db, String url) throws Exception {
        String miniId = insert(db, "minigames", "coins_1000", 2, 2200, "paid", 0, false, false);
        String miniBigId = insert(db, "minigames", "coins_5000", 3, 18000, "paid", 0, false, false);
        String duelId = insert(db, "duels", "coins_1000", 2, 77, "paid", 0, false, false);
        String survivalId = insert(db, "survival", "coins_1000", null, 9, "paid", 0, false, false);
        String customId = insert(db, "skyblock_2", "coins_1000", 1, 909, "paid", 0, false, false);
        String historicalPaidId = insert(db, "survival", "retired_paid_tier", null, 4321, "paid", 0, false, false);
        String historicalCreatedId = insert(db, "survival", "retired_created_tier", null, 9876, "created", 0, false, false);
        String historicalDeliveredId = insert(db, "survival", "retired_delivered_tier", null, 1111, "paid", 0, true, false);
        List<String> rejectedIds = new ArrayList<>();
        for (String mode : MODES) {
            // Без токена проверяем каждый фильтр SELECT/claim; с токеном — фильтры завершения.
            for (boolean claimed : List.of(false, true)) {
                rejectedIds.add(insert(db, mode, "coins_1000", 2, 2200, "created", 0, false, claimed));
                rejectedIds.add(insert(db, mode, "coins_1000", 2, 2200, "paid", 1, false, claimed));
                rejectedIds.add(insert(db, mode, "coins_1000", 2, 2200, "paid", 0, true, claimed));
            }
            insert(db, mode, "coins_1000", 2, 2200, "paid", 0, false, true);
        }
        insert(db, "classic", "coins_1000", 2, 2200, "paid", 0, false, false);
        insert(db, "hard", "coins_1000", 2, 2200, "paid", 0, false, false);
        insert(db, "unknown", "coins_1000", 2, 2200, "paid", 0, false, false);

        // Более поздний id имеет более раннюю оплату: paid_at должен быть первым ключом сортировки.
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(
                "UPDATE shop_orders SET paid_at=? WHERE order_id=?")) {
            ps.setTimestamp(1, Timestamp.valueOf("2025-12-31 00:00:00"));
            ps.setString(2, miniBigId);
            check(ps.executeUpdate() == 1, "Подготовка проверки ORDER BY paid_at");
        }

        Map<String, OrdersRewardSource> sources = new HashMap<>();
        for (String mode : MODES) {
            OrdersRewardSource source = source(db, url, mode);
            check(source.requiresClaim(), "Резерв обязателен: " + mode);
            sources.put(mode, source);
        }
        OrdersRewardSource mini = sources.get("minigames");
        OrdersRewardSource survival = sources.get("survival");
        List<RewardItem> miniRows = mini.fetchPending(50);
        check(ids(miniRows).equals(List.of(miniBigId, miniId)), "Фильтры MiniGames и ORDER BY paid_at");
        check(ids(mini.fetchPending(1)).equals(List.of(miniBigId)), "LIMIT после сортировки");
        check(ids(sources.get("duels").fetchPending(50)).equals(List.of(duelId)), "Изоляция Duels");
        check(ids(sources.get("skyblock_2").fetchPending(50)).equals(List.of(customId)), "Новый режим выбирает только свои заказы");
        check(ids(survival.fetchPending(50)).equals(List.of(survivalId, historicalPaidId)), "Survival и история в общей таблице");
        check(ids(source(db, url, "classic").fetchPending(50)).equals(List.of(survivalId, historicalPaidId)), "Алиас не читает DB server=classic");
        RewardItem historical = row(db, historicalPaidId);
        check(historical.attrs.containsKey("quantity") && historical.attrs.get("quantity") == null
                && historical.attrs.get("product_title") == null, "NULL не заменяется 0/1/tier");
        RewardItem fetchedHistorical = survival.fetchPending(50).get(1);
        check(fetchedHistorical.attrs.get("quantity") == null && fetchedHistorical.attrs.get("product_title") == null,
                "Источник сохраняет NULL исторических метаданных");

        YamlConfiguration defaults;
        try (var resource = Objects.requireNonNull(OrdersMysqlChecks.class.getResourceAsStream("/config.yml"));
             var reader = new InputStreamReader(resource, StandardCharsets.UTF_8)) {
            defaults = YamlConfiguration.loadConfiguration(reader);
        }
        OrdersConfig miniConfig = OrdersConfig.load(config(url, "minigames", null), LOG);
        for (RewardItem item : miniRows) {
            List<String> commands = defaults.getStringList("tiers." + item.tier);
            check(RewardExecutor.prepareCommands(item, commands, miniConfig)
                    .equals(List.of("money give SamePlayer " + item.getAttrAsString("grant_qty"))),
                    "Готовые 2200/18000 без умножения quantity");
        }
        OrdersConfig survivalConfig = OrdersConfig.load(config(url, "survival", null), LOG);
        List<String> historyCommands = List.of("points give ${nickname} ${grant_qty}");
        check(RewardExecutor.prepareCommands(fetchedHistorical, historyCommands, survivalConfig)
                .equals(List.of("points give SamePlayer 4321")), "Старый tier без product_title/quantity выдаётся по grant_qty");
        rejectPreparation(fetchedHistorical, List.of("points give ${nickname} ${quantity}"), survivalConfig);
        rejectPreparation(fetchedHistorical, List.of("say ${product_title}"), survivalConfig);
        rejectPreparation(fetchedHistorical, List.of(), survivalConfig);
        rejectPreparation(miniRows.getFirst(), historyCommands, survivalConfig);
        check(row(db, historicalPaidId).getAttrAsString("delivery_claim_token").isEmpty(), "Ошибки подготовки не резервируют заказ");

        // Фильтры UPDATE проверяются даже при правильном существующем токене.
        for (String id : rejectedIds) {
            RewardItem rejected = row(db, id);
            OrdersRewardSource source = sources.get(rejected.getAttrAsString("server"));
            String token = rejected.getAttrAsString("delivery_claim_token");
            String before = snapshot(db, -1);
            check(!tx(db, c -> source.claim(c, rejected, UUID.randomUUID().toString())), "Неготовая строка не резервируется");
            check(!tx(db, c -> source.markDelivered(c, rejected, token)), "Неготовая строка не завершается");
            check(!tx(db, c -> source.markFailed(c, rejected, token, "ошибка")), "Неготовая строка не изменяется");
            check(before.equals(snapshot(db, -1)), "Все строки сохранены после отклонённых UPDATE");
        }
        checkClaimRace(db, url, "survival", row(db, survivalId));
        checkClaimRace(db, url, "duels", row(db, duelId));
        checkClaimRace(db, url, "minigames", miniRows.getFirst());
        checkClaimRace(db, url, "skyblock_2", row(db, customId));

        // Завершённая перенесённая запись не выдаётся даже без токена.
        RewardItem alreadyDelivered = row(db, historicalDeliveredId);
        check(!tx(db, c -> survival.claim(c, alreadyDelivered, UUID.randomUUID().toString())), "Перенесённая выдача не повторяется");

        check(pay(db, historicalCreatedId, "survival") == 1, "Оплата перенесённого created-заказа");
        RewardItem newlyPaid = survival.fetchPending(50).stream()
                .filter(item -> item.orderId.equals(historicalCreatedId)).findFirst().orElseThrow();
        check(newlyPaid.tier.equals("retired_created_tier") && newlyPaid.attrs.get("quantity") == null,
                "Исходные tier/UUID/NULL после оплаты сохранены");
        check(RewardExecutor.prepareCommands(newlyPaid, historyCommands, survivalConfig)
                .equals(List.of("points give SamePlayer 9876")), "Оплаченная история использует исходный grant_qty");
        checkClaimRace(db, url, "survival", newlyPaid);
        String completed = snapshot(db, -1);
        check(pay(db, historicalCreatedId, "survival") == 0, "Повтор перехода created→paid не меняет оплаченный заказ");
        check(completed.equals(snapshot(db, -1)), "Повтор оплаты не снимает delivered_at/claim");
    }

    private static List<String> ids(List<RewardItem> rows) {
        return rows.stream().map(item -> item.orderId).toList();
    }

    private static void rejectPreparation(RewardItem item, List<String> commands, OrdersConfig config) throws Exception {
        try {
            RewardExecutor.prepareCommands(item, commands, config);
            throw new AssertionError("Небезопасная команда подготовлена");
        } catch (RewardExecutor.DeliveryException expected) {
            check(!expected.getMessage().isBlank(), "Диагностика подготовки");
        }
    }

    private static void checkClaimRace(DbPool db, String url, String mode, RewardItem item) throws Exception {
        OrdersRewardSource source = source(db, url, mode);
        String untouched = snapshot(db, item.id);
        String business = businessSnapshot(db, item.id);
        AtomicInteger commandCallbacks = new AtomicInteger();
        AtomicReference<String> owner = new AtomicReference<>();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService competitors = Executors.newFixedThreadPool(2);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                SafeConfig cfg = config(url, mode, null);
                cfg.getConfig().set("orders.workerId", "test-" + mode + "-" + i);
                OrdersRewardSource worker = new OrdersRewardSource(db, OrdersConfig.load(cfg, LOG));
                futures.add(competitors.submit(() -> {
                    start.await();
                    String token = UUID.randomUUID().toString();
                    if (tx(db, c -> worker.claim(c, item, token))) {
                        owner.set(token);
                        commandCallbacks.incrementAndGet(); // Имитируется только допуск после commit, без Bukkit.
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) future.get(30, TimeUnit.SECONDS);
        } finally {
            competitors.shutdownNow();
        }
        check(commandCallbacks.get() == 1, "Один победитель claim/commit: " + mode);
        String token = owner.get();
        String foreignToken = UUID.randomUUID().toString();
        check(!tx(db, c -> source.markDelivered(c, item, foreignToken)), "Чужой токен не завершает заказ");
        check(!tx(db, c -> source.markFailed(c, item, foreignToken, "ошибка")), "Чужой токен не меняет ошибку");
        RewardItem wrongOrder = new RewardItem(item.id, UUID.randomUUID().toString(), item.nickname, item.tier, item.amount, item.currency, item.attrs);
        check(!tx(db, c -> source.markDelivered(c, wrongOrder, token)), "Чужой order_id не завершает заказ");
        check(!tx(db, c -> source.markFailed(c, wrongOrder, token, "ошибка")), "Чужой order_id не меняет ошибку");
        String otherMode = mode.equals("duels") ? "minigames" : "duels";
        OrdersRewardSource other = source(db, url, otherMode);
        Map<String, Object> foreignAttrs = new HashMap<>(item.attrs);
        foreignAttrs.put("server", otherMode);
        RewardItem foreign = new RewardItem(item.id, item.orderId, item.nickname, item.tier, item.amount, item.currency, foreignAttrs);
        check(!tx(db, c -> source.markDelivered(c, foreign, token)), "Режим снимка не совпадает");
        check(!tx(db, c -> other.markDelivered(c, foreign, token)), "SQL server не совпадает");
        check(!tx(db, c -> other.markFailed(c, foreign, token, "ошибка")), "Ошибка чужого режима не записана");
        check(tx(db, c -> source.markFailed(c, item, token, "Неопределённый результат; сверить вручную")), "Владелец записывает ошибку");
        check(source.fetchPending(50).stream().noneMatch(row -> row.orderId.equals(item.orderId)), "Ошибка сохраняет токен; poll не повторяет выдачу");
        check(!tx(db, c -> source.claim(c, item, foreignToken)), "Резерв не захватывается повторно");
        check(tx(db, c -> source.markDelivered(c, item, token)), "Владелец завершает заказ");
        check(!tx(db, c -> source.markDelivered(c, item, token)), "Повтор результата ничего не меняет");
        check(!tx(db, c -> source.markFailed(c, item, token, "ошибка")), "Завершённая выдача неизменна");
        check(untouched.equals(snapshot(db, item.id)), "Другие строки не изменились");
        check(business.equals(businessSnapshot(db, item.id)), "server/order_id/tier/quantity/grant_qty/amount/status сохранены");
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT delivery_attempts,delivery_claim_token,delivery_worker,delivered_at,delivery_note FROM shop_orders WHERE id=?")) {
            ps.setLong(1, item.id);
            try (ResultSet rs = ps.executeQuery()) {
                check(rs.next() && rs.getInt(1) == 1 && token.equals(rs.getString(2))
                        && rs.getString(3).startsWith("test-" + mode + "-") && rs.getTimestamp(4) != null
                        && rs.getString(5).equals("ok"), "Одна попытка, владелец и итог сохранены");
            }
        }
    }

    static OrdersRewardSource source(DbPool db, String url, String mode) {
        return new OrdersRewardSource(db, OrdersConfig.load(config(url, mode, null), LOG));
    }

    static RewardItem row(DbPool db, String orderId) throws Exception {
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement("SELECT * FROM shop_orders WHERE order_id=?")) {
            ps.setString(1, orderId);
            try (ResultSet rs = ps.executeQuery()) {
                check(rs.next(), "Фикстура существует");
                Map<String, Object> attrs = new HashMap<>();
                attrs.put("server", rs.getString("server"));
                attrs.put("quantity", rs.getObject("quantity", Long.class));
                attrs.put("product_title", rs.getString("product_title"));
                attrs.put("grant_qty", rs.getLong("grant_qty"));
                attrs.put("delivery_claim_token", rs.getString("delivery_claim_token"));
                return new RewardItem(rs.getLong("id"), rs.getString("order_id"), rs.getString("nickname"),
                        rs.getString("tier"), rs.getDouble("amount"), rs.getString("currency"), attrs);
            }
        }
    }

    private static int pay(DbPool db, String orderId, String server) throws Exception {
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(
                "UPDATE shop_orders SET status='paid' WHERE order_id=? AND server=? AND status='created'")) {
            ps.setString(1, orderId);
            ps.setString(2, server);
            return ps.executeUpdate();
        }
    }

    static String snapshot(DbPool db, long exceptId) throws Exception {
        return snapshot(db, "SELECT * FROM shop_orders WHERE id <> ? ORDER BY id", exceptId);
    }

    private static String businessSnapshot(DbPool db, long id) throws Exception {
        return snapshot(db, "SELECT order_id,server,nickname,tier,quantity,product_title,grant_qty,amount,currency,status,paid_at,unitpay_id,is_test FROM shop_orders WHERE id=?", id);
    }

    private static String snapshot(DbPool db, String sql, long id) throws Exception {
        StringBuilder result = new StringBuilder();
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, id);
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

    static void check(boolean condition, String name) {
        if (!condition) throw new AssertionError(name);
    }
}
