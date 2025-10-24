package su.primecorp.primerewards.util;

public final class RateLimiter {
    private final double permitsPerSecond;
    private double stored;
    private long last;

    public RateLimiter(double qps) {
        this.permitsPerSecond = Math.max(0.1, qps);
        this.stored = permitsPerSecond;
        this.last = System.nanoTime();
    }

    public synchronized void acquire() {
        refill();
        if (stored >= 1.0) {
            stored -= 1.0;
            return;
        }
        long waitNanos = (long) ((1.0 - stored) / permitsPerSecond * 1_000_000_000L);
        if (waitNanos > 0) {
            try { Thread.sleep(waitNanos / 1_000_000L, (int)(waitNanos % 1_000_000L)); } catch (InterruptedException ignored) {}
        }
        refill();
        if (stored > 0) stored -= 1.0;
    }

    private void refill() {
        long now = System.nanoTime();
        double delta = (now - last) / 1_000_000_000.0;
        stored = Math.min(permitsPerSecond, stored + delta * permitsPerSecond);
        last = now;
    }
}
