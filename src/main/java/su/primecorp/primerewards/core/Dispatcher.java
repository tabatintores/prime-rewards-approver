package su.primecorp.primerewards.core;

import su.primecorp.primerewards.mysql.DbPool;
import su.primecorp.primerewards.util.RateLimiter;
import su.primecorp.primerewards.util.SafeConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public final class Dispatcher {
    private final RewardExecutor executor;
    private final List<RewardSource> sources;
    private final DbPool db;
    private final Logger log;
    private final ScheduledExecutorService scheduler;
    private final ThreadPoolExecutor workers;
    private final Semaphore parallelism;
    private final RateLimiter rateLimiter;
    private final long intervalMs, dbRetryBackoffMs, backoffBaseMs, backoffMaxMs, backoffJitterMs;
    private final int batchSize, dbMaxRetries;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final Set<String> inProgress = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, Backoff> backoffs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> nextLogAt = new ConcurrentHashMap<>();

    private record Backoff(long until, long delay) {}

    public Dispatcher(SafeConfig cfg, RewardExecutor executor, List<RewardSource> sources, Logger log, DbPool db) {
        this.executor = executor;
        this.sources = List.copyOf(sources);
        this.db = db;
        this.log = log;
        intervalMs = Math.max(250L, Math.min(300000L, cfg.getLong("polling.intervalMs", 3000L)));
        batchSize = Math.max(1, Math.min(500, cfg.getInt("polling.batchSize", 50)));
        int concurrency = Math.max(1, Math.min(32, cfg.getInt("polling.maxConcurrentDeliveries", 4)));
        dbMaxRetries = Math.max(1, Math.min(5, cfg.getInt("polling.dbMaxRetries", 3)));
        dbRetryBackoffMs = Math.max(50L, Math.min(5000L, cfg.getLong("polling.dbRetryBackoffMs", 300L)));
        backoffBaseMs = Math.max(250L, Math.min(300000L, cfg.getLong("backoff.baseMs", 2000L)));
        backoffMaxMs = Math.max(backoffBaseMs, Math.min(3600000L, cfg.getLong("backoff.maxMs", 120000L)));
        backoffJitterMs = Math.max(0L, Math.min(10000L, cfg.getLong("backoff.jitterMs", 500L)));
        double qps = cfg.getDouble("rateLimit.qps", 20.0);
        rateLimiter = new RateLimiter(Double.isFinite(qps) ? Math.max(0.1, Math.min(100, qps)) : 20);
        parallelism = new Semaphore(concurrency);
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "PrimeRewards-Dispatcher"));
        workers = new ThreadPoolExecutor(concurrency, concurrency, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(concurrency), r -> daemon(r, "PrimeRewards-Worker"));
    }

    private static Thread daemon(Runnable task, String name) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
    }

    public boolean isRunning() { return running.get(); }

    public void start() {
        if (running.compareAndSet(false, true)) {
            scheduler.scheduleWithFixedDelay(this::tickSafe, 0L, intervalMs, TimeUnit.MILLISECONDS);
        }
    }

    /** Основной поток: остановить выдачу немедленно; ожидание БД выполняется отдельно async. */
    public void stop() {
        running.set(false);
        executor.stop();
        scheduler.shutdownNow();
        workers.shutdown();
    }

    /** Только async. Новый источник нельзя создавать до успешного завершения старого. */
    public boolean awaitStopped(long timeoutSeconds) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        if (!scheduler.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) return false;
        long remaining = Math.max(0L, deadline - System.nanoTime());
        return workers.awaitTermination(remaining, TimeUnit.NANOSECONDS);
    }

    private void tickSafe() {
        if (!running.get()) return;
        try {
            for (RewardSource source : sources) {
                if (!running.get()) return;
                processSource(source);
            }
        } catch (Exception failure) {
            if (running.get()) log.warning("Ошибка опроса источников: " + safeFailure(failure));
        }
    }

    private void processSource(RewardSource source) {
        List<RewardItem> batch;
        try {
            batch = source.fetchPending(batchSize);
        } catch (Exception failure) {
            warn("fetch#" + source.name(), "Не удалось прочитать источник " + source.description() + ": " + safeFailure(failure));
            return;
        }
        for (RewardItem item : batch) {
            if (!running.get()) return;
            String key = source.name() + "#" + item.id;
            Backoff backoff = backoffs.get(key);
            if (backoff != null && backoff.until() > System.currentTimeMillis()) continue;
            if (!inProgress.add(key)) continue;
            if (!parallelism.tryAcquire()) {
                inProgress.remove(key);
                continue;
            }
            rateLimiter.acquire();
            if (!running.get()) {
                inProgress.remove(key);
                parallelism.release();
                return;
            }
            try {
                workers.execute(() -> deliver(source, item, key));
            } catch (RejectedExecutionException rejected) {
                inProgress.remove(key);
                parallelism.release();
            }
        }
    }

    private void deliver(RewardSource source, RewardItem item, String key) {
        boolean claimed = false;
        boolean claimAttempted = false;
        String token = source.requiresClaim() ? UUID.randomUUID().toString() : null;
        try {
            if (!running.get()) return;
            RewardExecutor.PreparedReward prepared = executor.prepare(item, source.name());
            if (!running.get()) return;
            if (source.requiresClaim()) {
                claimAttempted = true;
                // Не повторяем неоднозначный claim/commit. Без подтверждения команды запрещены.
                if (!transaction(tx -> source.claim(tx, item, token))) return;
                claimed = true;
                log.info("Заказ зарезервирован: " + source.context(item));
            }
            if (!running.get()) throw new RewardExecutor.DeliveryException("Остановка после резервирования; требуется сверка.");
            executor.execute(prepared);
            // DB retry повторяет только запись результата, никогда не игровые команды.
            if (!retryTransaction(tx -> source.markDelivered(tx, item, token))) {
                throw new RewardExecutor.DeliveryException("Подтверждение выдачи не записано; требуется сверка.");
            }
            delivered.incrementAndGet();
            backoffs.remove(key);
            nextLogAt.remove(key);
            log.info("Заказ/награда выданы: " + source.context(item));
        } catch (Exception failure) {
            failed.incrementAndGet();
            defer(key);
            String reason = safeFailure(failure);
            // Неизвестный tier / офлайн до claim не меняют shop_orders и не занимают токен.
            // При неопределённом commit ошибка запишется только если БД сохранила наш токен.
            if (claimAttempted || !source.requiresClaim()) {
                try {
                    if (!retryTransaction(tx -> source.markFailed(tx, item, token, reason))) {
                        warn("finish#" + key, "Запись ошибки не изменена (состояние или владелец уже другой): " + source.context(item));
                    }
                } catch (Exception dbFailure) {
                    warn("finish#" + key, "Не удалось сохранить результат: " + source.context(item) + "; " + safeFailure(dbFailure));
                }
            }
            String suffix = (claimed || claimAttempted)
                    ? " Автоснятие резерва запрещено; проверьте заказ вручную." : "";
            warn(key, source.context(item) + ": " + reason + suffix);
        } finally {
            inProgress.remove(key);
            parallelism.release();
        }
    }

    @FunctionalInterface
    private interface DbOperation { boolean run(Connection connection) throws Exception; }

    private boolean transaction(DbOperation operation) throws Exception {
        try (Connection connection = db.getConnection()) {
            connection.setAutoCommit(false);
            try {
                boolean changed = operation.run(connection);
                if (changed) connection.commit();
                else connection.rollback();
                return changed;
            } catch (Exception failure) {
                try { connection.rollback(); } catch (SQLException rollback) { failure.addSuppressed(rollback); }
                throw failure;
            }
        }
    }

    private boolean retryTransaction(DbOperation operation) throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                return transaction(operation);
            } catch (SQLException failure) {
                if (attempt >= dbMaxRetries) throw failure;
                Thread.sleep(dbRetryBackoffMs * attempt);
            }
        }
    }

    private void defer(String key) {
        if (backoffs.size() >= 10000 && !backoffs.containsKey(key)) backoffs.clear();
        backoffs.compute(key, (ignored, previous) -> {
            long delay = previous == null ? backoffBaseMs : Math.min(backoffMaxMs, previous.delay() * 2);
            long jitter = ThreadLocalRandom.current().nextLong(backoffJitterMs + 1);
            return new Backoff(System.currentTimeMillis() + Math.min(backoffMaxMs, delay + jitter), delay);
        });
    }

    private void warn(String key, String message) {
        long now = System.currentTimeMillis();
        if (nextLogAt.getOrDefault(key, 0L) > now) return;
        if (nextLogAt.size() >= 10000) nextLogAt.clear();
        nextLogAt.put(key, now + 60000L);
        log.warning(message);
    }

    public static String safeFailure(Throwable failure) {
        if (failure instanceof RewardExecutor.DeliveryException) return failure.getMessage();
        if (failure instanceof SQLException sql) return "Ошибка БД (SQLState=" + sql.getSQLState() + ", код=" + sql.getErrorCode() + ").";
        if (failure instanceof CancellationException || failure instanceof InterruptedException) return "Работа остановлена; результат требует проверки.";
        return "Ошибка обработки (" + failure.getClass().getSimpleName() + "); подробности проверьте локально без публикации секретов.";
    }

    public String dumpStats() {
        return String.format("§aВыдано:§f %d  §cОшибок:§f %d  §7Активных работ:§f %d  §7В очереди:§f %d",
                delivered.get(), failed.get(), workers.getActiveCount(), workers.getQueue().size());
    }
}
