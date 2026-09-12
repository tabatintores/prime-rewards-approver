package su.primecorp.primerewards.sources;

import su.primecorp.primerewards.config.OrdersConfig;
import su.primecorp.primerewards.core.RewardItem;
import su.primecorp.primerewards.core.RewardSource;
import su.primecorp.primerewards.mysql.DbPool;

import java.sql.*;
import java.util.*;

public final class OrdersRewardSource implements RewardSource {
    private final DbPool db;
    private final OrdersConfig config;
    private final String eligible;

    public OrdersRewardSource(DbPool db, OrdersConfig config) {
        this.db = db;
        this.config = config;
        this.eligible = "status = 'paid' AND "
                + (config.multiserver() ? "is_test = 0" : "COALESCE(is_test, 0) = 0")
                + " AND delivered_at IS NULL";
    }

    @Override public String name() { return "orders"; }
    @Override public boolean requiresClaim() { return config.multiserver(); }
    @Override public String description() {
        return "orders server=" + config.server() + " workerId=" + config.workerId();
    }

    @Override
    public String context(RewardItem item) {
        return "orders server=" + config.server() + " id=" + item.id
                + " order_id=" + safeLog(item.orderId) + " workerId=" + config.workerId();
    }

    @Override
    public List<RewardItem> fetchPending(int batchSize) throws Exception {
        String sql = "SELECT id, order_id, nickname, tier, grant_qty, amount, currency, delivery_attempts, paid_at, unitpay_id, is_test"
                + (config.multiserver() ? ", server, quantity, product_title, delivery_claim_token, delivery_claimed_at, delivery_worker" : "")
                + " FROM " + config.sqlTable() + " WHERE " + eligible
                + (config.multiserver() ? " AND server = ? AND delivery_claim_token IS NULL" : "")
                + " ORDER BY paid_at ASC, id ASC LIMIT ?";
        List<RewardItem> list = new ArrayList<>();
        try (Connection c = db.getConnection(); PreparedStatement ps = statement(c, sql)) {
            int index = 1;
            if (config.multiserver()) ps.setString(index++, config.server());
            ps.setInt(index, Math.max(1, Math.min(500, batchSize)));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> attrs = new HashMap<>();
                    attrs.put("server", config.multiserver() ? rs.getString("server") : config.server());
                    attrs.put("quantity", config.multiserver() ? rs.getLong("quantity") : 1L);
                    attrs.put("product_title", config.multiserver() ? safe(rs.getString("product_title")) : "");
                    attrs.put("paid_at", safe(rs.getTimestamp("paid_at")));
                    attrs.put("unitpay_id", safe(rs.getString("unitpay_id")));
                    attrs.put("is_test", rs.getObject("is_test"));
                    attrs.put("attempts", rs.getInt("delivery_attempts"));
                    attrs.put("grant_qty", rs.getLong("grant_qty"));
                    if (config.multiserver()) {
                        attrs.put("delivery_claim_token", safe(rs.getString("delivery_claim_token")));
                        attrs.put("delivery_claimed_at", safe(rs.getTimestamp("delivery_claimed_at")));
                        attrs.put("delivery_worker", safe(rs.getString("delivery_worker")));
                    }
                    list.add(new RewardItem(rs.getLong("id"), rs.getString("order_id"), rs.getString("nickname"),
                            rs.getString("tier"), rs.getDouble("amount"), safe(rs.getString("currency")), attrs));
                }
            }
        }
        return list;
    }

    @Override
    public boolean claim(Connection tx, RewardItem item, String token) throws Exception {
        if (!config.multiserver() || !matches(item) || !validToken(token)) return false;
        String sql = "UPDATE " + config.sqlTable()
                + " SET delivery_claim_token = ?, delivery_claimed_at = NOW(), delivery_worker = ?, delivery_attempts = delivery_attempts + 1"
                + " WHERE id = ? AND order_id = ? AND server = ? AND " + eligible + " AND delivery_claim_token IS NULL";
        try (PreparedStatement ps = statement(tx, sql)) {
            ps.setString(1, token);
            ps.setString(2, config.workerId());
            ps.setLong(3, item.id);
            ps.setString(4, item.orderId);
            ps.setString(5, config.server());
            return ps.executeUpdate() == 1;
        }
    }

    @Override
    public boolean markDelivered(Connection tx, RewardItem item, String token) throws Exception {
        return finish(tx, item, token, null);
    }

    @Override
    public boolean markFailed(Connection tx, RewardItem item, String token, String reason) throws Exception {
        return finish(tx, item, token, safeLog(reason));
    }

    private boolean finish(Connection tx, RewardItem item, String token, String reason) throws Exception {
        if (!matches(item) || (config.multiserver() && !validToken(token))) return false;
        String sets = reason == null ? "delivered_at = NOW(), delivery_note = 'ok'" : "delivery_note = ?";
        if (!config.multiserver()) sets += ", delivery_attempts = delivery_attempts + 1";
        String sql = "UPDATE " + config.sqlTable() + " SET " + sets
                + " WHERE id = ? AND order_id = ? AND " + eligible
                + (config.multiserver() ? " AND server = ? AND delivery_claim_token = ?" : "");
        try (PreparedStatement ps = statement(tx, sql)) {
            int index = 1;
            if (reason != null) ps.setString(index++, reason);
            ps.setLong(index++, item.id);
            ps.setString(index++, item.orderId);
            if (config.multiserver()) {
                ps.setString(index++, config.server());
                ps.setString(index, token);
            }
            return ps.executeUpdate() == 1;
        }
    }

    private boolean matches(RewardItem item) {
        return item.id > 0 && item.orderId != null && config.server().equals(item.getAttrAsString("server"));
    }

    private static boolean validToken(String token) {
        return token != null && token.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    }

    private PreparedStatement statement(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        try {
            statement.setQueryTimeout(config.queryTimeoutSeconds());
            return statement;
        } catch (SQLException failure) {
            statement.close();
            throw failure;
        }
    }

    private static String safe(Object value) { return value == null ? "" : String.valueOf(value); }

    private static String safeLog(String value) {
        String text = safe(value).replaceAll("[\\p{Cntrl}]", " ");
        return text.substring(0, Math.min(240, text.length()));
    }
}
