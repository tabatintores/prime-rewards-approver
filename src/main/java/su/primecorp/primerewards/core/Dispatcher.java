package su.primecorp.primerewards.core;

import org.bukkit.plugin.Plugin;
import su.primecorp.primerewards.mysql.DbPool;
import su.primecorp.primerewards.util.RateLimiter;
import su.primecorp.primerewards.util.SafeConfig;

import java.sql.Connection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Dispatcher {

    private final Plugin plugin;
    private final RewardExecutor executor;
    private final List<RewardSource> sources;
    private final DbPool db;
    private final Logger log;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "PrimeRewards-Dispatcher");
        t.setDaemon(true);
        return t;
    });
    private ExecutorService workers;
    private Semaphore parallelism;
    private RateLimiter rateLimiter;

    // cfg
    private volatile long intervalMs;
    private volatile int batchSize;
    private volatile int dbMaxRetries;
    private volatile long dbRetryBackoffMs;
    private volatile long backoffBaseMs;
    private volatile long backoffMaxMs;
    private volatile long backoffJitterMs;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    // per-item backoff (in-memory)
    private final ConcurrentHashMap<Long, Long> nextAllowedAtMillis = new ConcurrentHashMap<>();
    private final Random random = new Random();

    public Dispatcher(Plugin plugin, SafeConfig cfg, RewardExecutor executor,
                      List<RewardSource> sources, Logger log, DbPool dbPool) {
        this.plugin = plugin;
        this.executor = executor;
        this.sources = sources;
        this.db = dbPool;
        this.log = log;
        applyConfig(cfg);
    }

    public void reload(SafeConfig cfg) {
        applyConfig(cfg);
        log.info("Dispatcher reloaded: interval=" + intervalMs + "ms, batch=" + batchSize);
    }

    private void applyConfig(SafeConfig cfg) {
        this.intervalMs = cfg.getLong("polling.intervalMs", 3000L);
        this.batchSize = cfg.getInt("polling.batchSize", 50);
        int maxConc = cfg.getInt("polling.maxConcurrentDeliveries", 4);
        this.dbMaxRetries = cfg.getInt("polling.dbMaxRetries", 3);
        this.dbRetryBackoffMs = cfg.getLong("polling.dbRetryBackoffMs", 300L);

        this.backoffBaseMs = cfg.getLong("backoff.baseMs", 2000L);
        this.backoffMaxMs = cfg.getLong("backoff.maxMs", 120000L);
        this.backoffJitterMs = cfg.getLong("backoff.jitterMs", 500L);

        this.parallelism = new Semaphore(Math.max(1, maxConc));

        if (workers != null) workers.shutdownNow();
        int poolSize = Math.max(2, maxConc);
        this.workers = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "PrimeRewards-Worker");
            t.setDaemon(true);
            return t;
        });

        double qps = cfg.getDouble("rateLimit.qps", 20.0);
        this.rateLimiter = new RateLimiter(qps);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        scheduler.scheduleWithFixedDelay(this::tickSafe, 0L, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void stopAndWait() {
        running.set(false);
        scheduler.shutdown();
        try { scheduler.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        if (workers != null) {
            workers.shutdown();
            try { workers.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }
    }

    private void tickSafe() {
        if (!running.get()) return;
        try {
            for (RewardSource src : sources) {
                processSource(src);
            }
        } catch (Throwable t) {
            log.log(Level.SEVERE, "Dispatcher tick failed", t);
        }
    }

    private void processSource(RewardSource src) {
        List<RewardItem> batch;
        try {
            batch = src.fetchPending(batchSize);
        } catch (Exception e) {
            log.warning("fetchPending failed for " + src.name() + ": " + e.getMessage());
            return;
        }
        if (batch.isEmpty()) return;

        for (RewardItem item : batch) {
            long now = System.currentTimeMillis();
            long gate = nextAllowedAtMillis.getOrDefault(item.id, 0L);
            if (gate > now) continue; // backoff not elapsed yet

            if (!parallelism.tryAcquire()) continue;
            rateLimiter.acquire(); // protect QPS

            workers.submit(() -> {
                try (Connection tx = db.getConnection()) {
                    tx.setAutoCommit(false);
                    try {
                        // execute reward actions
                        executor.execute(item);

                        boolean ok = retryDb(() -> src.markDelivered(tx, item.id));
                        if (!ok) throw new RuntimeException("MarkDelivered returned false (concurrent update?)");

                        tx.commit();
                        delivered.incrementAndGet();
                        log.info("[OK] " + src.name() + " id=" + item.id + " order_id=" + item.orderId + " tier=" + item.tier + " nick=" + item.nickname);
                        nextAllowedAtMillis.remove(item.id);
                    } catch (Exception ex) {
                        try { tx.rollback(); } catch (Exception ignore) {}

                        long delay = computeNextBackoff(item.id);
                        nextAllowedAtMillis.put(item.id, System.currentTimeMillis() + delay);

                        try (Connection tx2 = db.getConnection()) {
                            tx2.setAutoCommit(false);
                            String reason = trimReason(ex.getMessage());
                            boolean marked = retryDb(() -> src.markFailed(tx2, item.id, reason));
                            if (marked) tx2.commit(); else tx2.rollback();
                        } catch (Exception dbEx) {
                            log.warning("markFailed error for id=" + item.id + ": " + dbEx.getMessage());
                        }
                        failed.incrementAndGet();
                        log.warning("[FAIL] " + src.name() + " id=" + item.id + " " + ex.getMessage());
                    }
                } catch (Exception outer) {
                    log.warning("Worker fatal for id=" + item.id + ": " + outer.getMessage());
                } finally {
                    parallelism.release();
                }
            });
        }
    }

    private String trimReason(String msg) {
        if (msg == null) return "error";
        msg = msg.replaceAll("[\\r\\n\\t]+", " ").trim();
        return msg.length() > 240 ? msg.substring(0, 240) : msg;
    }

    private long computeNextBackoff(long id) {
        long now = System.currentTimeMillis();
        long prev = nextAllowedAtMillis.getOrDefault(id, 0L);
        long waited = Math.max(0, prev - now);
        long next = (waited == 0 ? backoffBaseMs : Math.min(backoffMaxMs, waited * 2));
        long jitter = (backoffJitterMs > 0) ? (long)(random.nextDouble() * (backoffJitterMs + 1)) : 0;
        return Math.min(backoffMaxMs, next + jitter);
    }

    private boolean retryDb(Callable<Boolean> op) throws Exception {
        int attempts = 0;
        while (true) {
            try {
                return op.call();
            } catch (Exception e) {
                attempts++;
                if (attempts >= dbMaxRetries) throw e;
                Thread.sleep(dbRetryBackoffMs * attempts);
            }
        }
    }

    public String dumpStats() {
        int active = (workers instanceof ThreadPoolExecutor tpe) ? tpe.getActiveCount() : -1;
        int queued = (workers instanceof ThreadPoolExecutor tpe) ? tpe.getQueue().size() : -1;
        return String.format("§aDelivered:§f %d  §cFailed:§f %d  §7WorkersActive:§f %d  §7Queue:§f %d",
                delivered.get(), failed.get(), active, queued);
    }
}
