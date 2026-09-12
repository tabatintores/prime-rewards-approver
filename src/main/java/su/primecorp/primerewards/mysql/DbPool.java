package su.primecorp.primerewards.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import su.primecorp.primerewards.util.SafeConfig;

import java.sql.Connection;
import java.sql.SQLException;

public final class DbPool implements AutoCloseable {
    private final HikariDataSource ds;

    /** Снимок настроек создаётся sync; конструктор пула и close вызываются только async. */
    public record Settings(String jdbcUrl, String username, String password, int minimumIdle,
                           int maximumPoolSize, long connectionTimeoutMs, long validationTimeoutMs,
                           long socketTimeoutMs) {
        public static Settings from(SafeConfig cfg) {
            int max = Math.max(1, Math.min(16, cfg.getInt("mysql.maximumPoolSize", 2)));
            long connection = Math.max(1000L, Math.min(30000L, cfg.getLong("mysql.connectionTimeoutMs", 5000L)));
            return new Settings(cfg.getString("mysql.jdbcUrl"), cfg.getString("mysql.username"),
                    cfg.getString("mysql.password"), Math.max(0, Math.min(max, cfg.getInt("mysql.minimumIdle", 0))),
                    max, connection, Math.max(250L, Math.min(connection, cfg.getLong("mysql.validationTimeoutMs", 3000L))),
                    Math.max(1000L, Math.min(60000L, cfg.getLong("mysql.socket_timeout_ms", 15000L))));
        }
    }

    public DbPool(Settings settings) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(settings.jdbcUrl());
        hc.setUsername(settings.username());
        hc.setPassword(settings.password());
        hc.setMinimumIdle(settings.minimumIdle());
        hc.setMaximumPoolSize(settings.maximumPoolSize());
        hc.setConnectionTimeout(settings.connectionTimeoutMs());
        hc.setValidationTimeout(settings.validationTimeoutMs());
        hc.setInitializationFailTimeout(-1);
        hc.addDataSourceProperty("connectTimeout", String.valueOf(settings.connectionTimeoutMs()));
        hc.addDataSourceProperty("socketTimeout", String.valueOf(settings.socketTimeoutMs()));
        hc.setPoolName("PrimeRewardsPool");
        this.ds = new HikariDataSource(hc);
    }

    public Connection getConnection() throws SQLException { return ds.getConnection(); }

    @Override public void close() { ds.close(); }
}
