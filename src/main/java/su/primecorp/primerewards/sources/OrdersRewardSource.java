package su.primecorp.primerewards.sources;

import su.primecorp.primerewards.core.RewardItem;
import su.primecorp.primerewards.core.RewardSource;
import su.primecorp.primerewards.mysql.DbPool;
import su.primecorp.primerewards.util.SafeConfig;

import java.sql.*;
import java.util.*;

public final class OrdersRewardSource implements RewardSource {
    private final DbPool db;
    private final java.util.logging.Logger log;

    public OrdersRewardSource(DbPool db, SafeConfig cfg, java.util.logging.Logger log) {
        this.db = db;
        this.log = log;
    }

    @Override
    public String name() { return "orders"; }

    @Override
    public List<RewardItem> fetchPending(int batchSize) throws Exception {
        String sql =
                "SELECT id, order_id, nickname, tier, amount, currency, status, delivered_at, delivery_attempts, paid_at, unitpay_id, is_test " +
                        "FROM external_data.orders " +
                        "WHERE status='paid' AND delivered_at IS NULL " +
                        "ORDER BY paid_at ASC " +
                        "LIMIT ?";
        List<RewardItem> list = new ArrayList<>();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setInt(1, batchSize);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long id = rs.getLong("id");
                    String orderId = rs.getString("order_id");
                    String nick = rs.getString("nickname");
                    String tier = rs.getString("tier");
                    double amount = rs.getDouble("amount");
                    String currency = safe(rs.getString("currency"));

                    Map<String, Object> attrs = new HashMap<>();
                    attrs.put("paid_at", safe(rs.getTimestamp("paid_at")));
                    attrs.put("unitpay_id", safe(rs.getString("unitpay_id")));
                    attrs.put("is_test", rs.getObject("is_test"));
                    attrs.put("attempts", rs.getInt("delivery_attempts"));

                    list.add(new RewardItem(id, orderId, nick, tier, amount, currency, attrs));
                }
            }
        }
        return list;
    }

    @Override
    public boolean markDelivered(Connection txConn, long id) throws Exception {
        String sql =
                "UPDATE external_data.orders " +
                        "SET delivered_at = NOW(), delivery_attempts = delivery_attempts + 1, delivery_note = 'ok' " +
                        "WHERE id = ? AND delivered_at IS NULL";
        try (PreparedStatement ps = txConn.prepareStatement(sql)) {
            ps.setLong(1, id);
            int updated = ps.executeUpdate();
            return updated == 1;
        }
    }

    @Override
    public boolean markFailed(Connection txConn, long id, String reason) throws Exception {
        String sql =
                "UPDATE external_data.orders " +
                        "SET delivery_attempts = delivery_attempts + 1, delivery_note = ? " +
                        "WHERE id = ? AND delivered_at IS NULL";
        try (PreparedStatement ps = txConn.prepareStatement(sql)) {
            ps.setString(1, reason);
            ps.setLong(2, id);
            int updated = ps.executeUpdate();
            return updated == 1;
        }
    }

    private static String safe(Object o) {
        return o == null ? "" : String.valueOf(o);
    }
}
