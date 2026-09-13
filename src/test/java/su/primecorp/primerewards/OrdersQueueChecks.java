package su.primecorp.primerewards;

import su.primecorp.primerewards.config.OrdersConfig;
import su.primecorp.primerewards.core.*;
import su.primecorp.primerewards.mysql.DbPool;
import su.primecorp.primerewards.sources.OrdersRewardSource;
import su.primecorp.primerewards.util.SafeConfig;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static su.primecorp.primerewards.OrdersMysqlChecks.*;

/** Ручные регрессии: настоящий Dispatcher/MySQL, управляемая замена только игровых действий. */
final class OrdersQueueChecks {
    private static final Logger LOG = Logger.getLogger("OrdersQueueChecks");
    private static final int BATCH = 3;

    static void run(DbPool db, String url) throws Exception {
        checkNullableCursor(db, url);
        checkContinuousArrivals(db, url);
        checkBlockedHead(db, url, false);
        checkBlockedHead(db, url, true);
        checkBackoff(db, url);
        checkCompetingDispatchers(db, url);
        checkBusyWorkers(db, url);
    }

    private static String order(DbPool db, String mode, String tier, int grant) throws Exception {
        return insert(db, mode, tier, null, grant, "paid", 0, false, false);
    }

    private static void paidAt(DbPool db, String id, String value) throws Exception {
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(
                "UPDATE shop_orders SET paid_at=? WHERE order_id=?")) {
            ps.setTimestamp(1, value == null ? null : Timestamp.valueOf(value));
            ps.setString(2, id);
            check(ps.executeUpdate() == 1, "Дата фикстуры обновлена");
        }
    }

    private static void checkNullableCursor(DbPool db, String url) throws Exception {
        String mode = "queue_null";
        String late = order(db, mode, "known", 1);
        String nullA = order(db, mode, "known", 2);
        String earlyA = order(db, mode, "known", 3);
        String nullB = order(db, mode, "known", 4);
        String earlyB = order(db, mode, "known", 5);
        paidAt(db, nullA, null);
        paidAt(db, nullB, null);
        paidAt(db, earlyA, "2025-01-01 00:00:00");
        paidAt(db, earlyB, "2025-01-01 00:00:00");
        OrdersRewardSource cursor = source(db, url, mode);
        check(ids(cursor.fetchPending(2)).equals(List.of(nullA, nullB)), "NULL paid_at: порядок id");
        check(ids(cursor.fetchPending(2)).equals(List.of(earlyA, earlyB)), "Переход NULL → дата и одинаковые paid_at");
        check(ids(cursor.fetchPending(2)).equals(List.of(late)), "Курсор даты идёт дальше независимо от id");
        check(ids(cursor.fetchPending(2)).equals(List.of(nullA, nullB)), "Неполная страница возвращает к началу");
        check(ids(source(db, url, mode).fetchPending(1)).equals(List.of(nullA)), "Новый источник/reload не наследует курсор");

        // Ровно полная последняя страница: пустой следующий опрос завершает обход.
        OrdersRewardSource exact = source(db, url, mode);
        check(exact.fetchPending(5).size() == 5, "Полная последняя страница");
        check(exact.fetchPending(5).isEmpty(), "Конец обхода не перечитывает начало в том же опросе");
        check(exact.fetchPending(5).size() == 5, "Следующий опрос начинает новый обход");
    }

    private static void checkContinuousArrivals(DbPool db, String url) throws Exception {
        String mode = "queue_arrivals";
        List<String> original = new ArrayList<>();
        for (int i = 0; i < 5; i++) original.add(order(db, mode, "known", i + 1));
        OrdersRewardSource cursor = source(db, url, mode);
        check(ids(cursor.fetchPending(2)).equals(original.subList(0, 2)), "Начало конечного обхода");
        String newId = order(db, mode, "known", 100);
        // Даже вставка с NULL позади курсора не попадёт в текущий снимок id.
        paidAt(db, newId, null);
        check(ids(cursor.fetchPending(2)).equals(original.subList(2, 4)), "Новая покупка не меняет середину обхода");
        order(db, mode, "known", 101);
        check(ids(cursor.fetchPending(2)).equals(original.subList(4, 5)), "Постоянные вставки не растягивают конец обхода");
        order(db, mode, "known", 102);
        check(ids(cursor.fetchPending(2)).equals(List.of(newId, original.getFirst())),
                "Следующий обход видит новую покупку и повторяет старую при продолжающихся вставках");
    }

    private static void checkBlockedHead(DbPool db, String url, boolean missingTier) throws Exception {
        String mode = missingTier ? "queue_mapping" : "queue_offline";
        Set<String> blocked = new HashSet<>();
        Set<String> offlineNames = new HashSet<>();
        for (int i = 0; i < BATCH * 2 + 1; i++) {
            String id = order(db, mode, missingTier ? "missing" : "known", i + 1);
            blocked.add(id);
            nickname(db, id, "Offline" + i);
            offlineNames.add("Offline" + i);
        }
        String available = order(db, mode, "known", 777);
        FakeDelivery delivery = new FakeDelivery(db, OrdersConfig.load(config(url, mode, null), LOG));
        if (!missingTier) delivery.offline.addAll(offlineNames);
        TrackingSource source = new TrackingSource(source(db, url, mode));
        Dispatcher dispatcher = dispatcher(db, url, mode, delivery, source, 250);
        try {
            dispatcher.start();
            await(() -> delivered(db, available), "Заказ после двух заблокированных страниц должен выдаться");
            check(source.pages.size() >= 3, "Продвижение происходит через последовательные опросы");
            check(source.pages.getFirst().size() == BATCH, "Первая пачка полностью заблокирована");
            for (String id : blocked) {
                check(row(db, id).getAttrAsString("delivery_claim_token").isEmpty(), "Пропущенный заказ не резервируется");
            }
            if (!missingTier) {
                String retry = blocked.iterator().next();
                delivery.offline.clear();
                await(() -> delivered(db, retry), "Отложенный заказ после появления игрока должен выдаться");
                check(source.visits(retry) >= 2, "Повторная попытка проходит после возврата курсора");
                check(delivery.count(retry) == 1, "Повторный обход не дублирует выдачу");
            }
        } finally {
            stop(dispatcher);
        }
    }

    private static void checkBackoff(DbPool db, String url) throws Exception {
        String mode = "queue_backoff";
        List<String> blocked = new ArrayList<>();
        for (int i = 0; i < BATCH; i++) blocked.add(order(db, mode, "known", i + 1));
        String tail = order(db, mode, "known", 777);
        nickname(db, tail, "OnlinePlayer");
        FakeDelivery delivery = new FakeDelivery(db, OrdersConfig.load(config(url, mode, null), LOG));
        delivery.offline.add("SamePlayer");
        TrackingSource source = new TrackingSource(source(db, url, mode));
        Dispatcher dispatcher = dispatcher(db, url, mode, delivery, source, 60000);
        try {
            dispatcher.start();
            await(() -> delivered(db, tail), "Хвост выдаётся после ошибок первой пачки");
            delivery.offline.clear();
            await(() -> source.visits(blocked.getFirst()) >= 2, "Повторный обход возвращает пачку в backoff");
            int polls = source.pages.size();
            await(() -> source.pages.size() > polls, "Опрос после полностью пропущенной пачки продолжается");
            for (String id : blocked) {
                check(delivery.preparations.get(id).get() == 1, "Настоящий backoff Dispatcher не допускает повторный prepare");
                check(delivery.count(id) == 0, "Backoff не выполняет команду");
            }
            String fresh = order(db, mode, "known", 888);
            await(() -> delivered(db, fresh), "Новая покупка проходит за пачкой, целиком находящейся в backoff");
        } finally {
            stop(dispatcher);
        }
    }

    private static void checkCompetingDispatchers(DbPool db, String url) throws Exception {
        String mode = "queue_race";
        for (int i = 0; i < BATCH; i++) order(db, mode, "missing", i + 1);
        String target = order(db, mode, "known", 777);
        order(db, "queue_other", "known", 777); // Тот же nickname/tier; все чужие строки должны сохраниться.
        long targetId = row(db, target).id;
        String untouched = snapshot(db, targetId);
        Map<String, AtomicInteger> executions = new ConcurrentHashMap<>();
        CountDownLatch bothPrepared = new CountDownLatch(2);
        FakeDelivery left = new FakeDelivery(db, OrdersConfig.load(config(url, mode, null), LOG));
        FakeDelivery right = new FakeDelivery(db, OrdersConfig.load(config(url, mode, null), LOG));
        left.executions = executions;
        right.executions = executions;
        left.raceOrder = right.raceOrder = target;
        left.raceGate = right.raceGate = bothPrepared;
        TrackingSource first = new TrackingSource(source(db, url, mode));
        SafeConfig secondConfig = config(url, mode, null);
        secondConfig.getConfig().set("orders.workerId", "queue-race-second");
        TrackingSource second = new TrackingSource(new OrdersRewardSource(db, OrdersConfig.load(secondConfig, LOG)));
        Dispatcher one = dispatcher(db, url, mode, left, first, 60000);
        Dispatcher two = dispatcher(db, url, mode, right, second, 60000);
        try {
            one.start();
            two.start();
            await(() -> delivered(db, target), "Конкурирующие обработчики доходят до доступного хвоста");
            await(() -> first.pages.size() >= 3 && second.pages.size() >= 3, "Оба обработчика повторно обходят очередь");
            check(bothPrepared.getCount() == 0, "Оба обработчика прочитали и подготовили один заказ до claim");
            check(left.count(target) == 1, "Команда допущена только одним владельцем claim");
            check(untouched.equals(snapshot(db, targetId)), "Другие серверы и необрабатываемые строки не изменены");
            try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(
                    "SELECT delivery_attempts,delivery_claim_token FROM shop_orders WHERE order_id=?")) {
                ps.setString(1, target);
                try (ResultSet rs = ps.executeQuery()) {
                    check(rs.next() && rs.getInt(1) == 1 && rs.getString(2) != null, "Одна попытка; токен сохранён");
                }
            }
        } finally {
            stop(one);
            stop(two);
        }
    }

    private static void nickname(DbPool db, String id, String name) throws Exception {
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(
                "UPDATE shop_orders SET nickname=? WHERE order_id=?")) {
            ps.setString(1, name);
            ps.setString(2, id);
            check(ps.executeUpdate() == 1, "Ник фикстуры обновлён");
        }
    }

    private static void checkBusyWorkers(DbPool db, String url) throws Exception {
        String mode = "queue_busy";
        for (int i = 0; i < BATCH; i++) order(db, mode, "known", i + 1);
        String tail = order(db, mode, "known", 777);
        FakeDelivery delivery = new FakeDelivery(db, OrdersConfig.load(config(url, mode, null), LOG));
        CountDownLatch gate = new CountDownLatch(1);
        delivery.executionGate = gate;
        TrackingSource source = new TrackingSource(source(db, url, mode));
        Dispatcher dispatcher = dispatcher(db, url, mode, delivery, source, 250);
        try {
            dispatcher.start();
            await(() -> delivery.executions.size() == BATCH, "Все воркеры заняты игровыми операциями");
            Thread.sleep(750); // Несколько интервалов: при нуле свободных мест SELECT выполняться не должен.
            check(source.pages.size() == 1, "Занятые воркеры не позволяют прочитать и пропустить хвост");
            gate.countDown();
            await(() -> delivered(db, tail), "После освобождения воркеров обработан следующий заказ");
            check(delivery.count(tail) == 1, "Хвост обработан один раз");
        } finally {
            gate.countDown();
            stop(dispatcher);
        }
    }

    private static Dispatcher dispatcher(DbPool db, String url, String mode, FakeDelivery delivery,
                                         TrackingSource source, long backoffMs) {
        SafeConfig cfg = config(url, mode, null);
        cfg.getConfig().set("polling.intervalMs", 250);
        cfg.getConfig().set("polling.batchSize", BATCH);
        cfg.getConfig().set("polling.maxConcurrentDeliveries", BATCH);
        cfg.getConfig().set("rateLimit.qps", 100);
        cfg.getConfig().set("backoff.baseMs", backoffMs);
        cfg.getConfig().set("backoff.maxMs", backoffMs);
        cfg.getConfig().set("backoff.jitterMs", 0);
        return new Dispatcher(cfg, delivery, List.of(source), LOG, db);
    }

    private static void stop(Dispatcher dispatcher) throws Exception {
        dispatcher.stop();
        check(dispatcher.awaitStopped(10), "Воркеры регрессионной проверки завершены");
    }

    private static boolean delivered(DbPool db, String id) throws Exception {
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT delivered_at FROM shop_orders WHERE order_id=?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getTimestamp(1) != null;
            }
        }
    }

    @FunctionalInterface private interface Condition { boolean ready() throws Exception; }

    private static void await(Condition condition, String message) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.ready()) {
            if (System.nanoTime() >= end) throw new AssertionError(message);
            Thread.sleep(20);
        }
    }

    private static List<String> ids(List<RewardItem> items) {
        return items.stream().map(item -> item.orderId).toList();
    }

    private static final class TrackingSource implements RewardSource {
        private final OrdersRewardSource delegate;
        private final List<List<String>> pages = new CopyOnWriteArrayList<>();

        private TrackingSource(OrdersRewardSource delegate) { this.delegate = delegate; }
        private int visits(String id) { return (int) pages.stream().filter(page -> page.contains(id)).count(); }
        @Override public String name() { return delegate.name(); }
        @Override public boolean requiresClaim() { return true; }
        @Override public String description() { return delegate.description(); }
        @Override public String context(RewardItem item) { return delegate.context(item); }
        @Override public List<RewardItem> fetchPending(int batchSize) throws Exception {
            List<RewardItem> page = delegate.fetchPending(batchSize);
            check(page.size() <= batchSize && batchSize <= BATCH, "Не более одной ограниченной страницы за опрос");
            pages.add(ids(page));
            return page;
        }
        @Override public boolean claim(Connection tx, RewardItem item, String token) throws Exception {
            return delegate.claim(tx, item, token);
        }
        @Override public boolean markDelivered(Connection tx, RewardItem item, String token) throws Exception {
            return delegate.markDelivered(tx, item, token);
        }
        @Override public boolean markFailed(Connection tx, RewardItem item, String token, String reason) throws Exception {
            return delegate.markFailed(tx, item, token, reason);
        }
    }

    private static final class FakeDelivery implements RewardDelivery {
        private final DbPool db;
        private final OrdersConfig config;
        private final Set<String> offline = ConcurrentHashMap.newKeySet();
        private final Map<String, AtomicInteger> preparations = new ConcurrentHashMap<>();
        private Map<String, AtomicInteger> executions = new ConcurrentHashMap<>();
        private String raceOrder;
        private CountDownLatch raceGate;
        private CountDownLatch executionGate;

        private FakeDelivery(DbPool db, OrdersConfig config) { this.db = db; this.config = config; }

        @Override public RewardExecutor.PreparedReward prepare(RewardItem item, String sourceName) throws Exception {
            preparations.computeIfAbsent(item.orderId, ignored -> new AtomicInteger()).incrementAndGet();
            List<String> mapping = item.tier.equals("known") ? List.of("test_grant ${order_id} ${grant_qty}") : List.of();
            List<String> commands = RewardExecutor.prepareCommands(item, mapping, config);
            // Заменяется только наличие игрока; prepare/mapping, backoff, курсор и claim настоящие.
            if (offline.contains(item.nickname)) throw new RewardExecutor.DeliveryException("Игрок офлайн в тесте.");
            if (item.orderId.equals(raceOrder)) {
                raceGate.countDown();
                check(raceGate.await(10, TimeUnit.SECONDS), "Оба обработчика должны дойти до claim");
            }
            return new RewardExecutor.PreparedReward(commands, item.nickname, true, item.getAttrAsString("server"));
        }

        @Override public void execute(RewardExecutor.PreparedReward reward) throws Exception {
            String id = reward.commands().getFirst().split(" ")[1];
            RewardItem stored = row(db, id);
            check(!stored.getAttrAsString("delivery_claim_token").isEmpty(), "До игровой операции claim уже закоммичен");
            check(stored.getAttrAsString("server").equals(reward.server()), "Режим команды совпадает с заказом");
            executions.computeIfAbsent(id, ignored -> new AtomicInteger()).incrementAndGet();
            if (executionGate != null) check(executionGate.await(10, TimeUnit.SECONDS), "Игровая операция разблокирована");
        }

        private int count(String id) { return executions.getOrDefault(id, new AtomicInteger()).get(); }
        @Override public void stop() {}
    }
}
