package su.primecorp.primerewards.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import su.primecorp.primerewards.util.SafeConfig;

import java.sql.Connection;
import java.sql.SQLException;

public final class DbPool implements AutoCloseable {
    private final HikariDataSource ds;

    public DbPool(SafeConfig cfg) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(cfg.getString("mysql.jdbcUrl"));
        hc.setUsername(cfg.getString("mysql.username"));
        hc.setPassword(cfg.getString("mysql.password"));
        hc.setMinimumIdle(cfg.getInt("mysql.minimumIdle", 1));
        hc.setMaximumPoolSize(cfg.getInt("mysql.maximumPoolSize", 5));
        hc.setConnectionTimeout(cfg.getLong("mysql.connectionTimeoutMs", 5000L));
        hc.setValidationTimeout(cfg.getLong("mysql.validationTimeoutMs", 3000L));
        hc.setPoolName("PrimeRewardsPool");
        this.ds = new HikariDataSource(hc);
    }

    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    @Override
    public void close() {
        ds.close();
    }
}
