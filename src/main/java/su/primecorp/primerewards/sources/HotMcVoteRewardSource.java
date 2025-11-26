package su.primecorp.primerewards.sources;

import su.primecorp.primerewards.core.RewardItem;
import su.primecorp.primerewards.core.RewardSource;
import su.primecorp.primerewards.mysql.DbPool;
import su.primecorp.primerewards.util.SafeConfig;

import java.sql.*;
import java.util.*;

public final class HotMcVoteRewardSource implements RewardSource {

    private final DbPool db;
    private final java.util.logging.Logger log;

    private final String tableName;
    private final String whereReady;
    private final String orderBy;
    private final String defaultTier;
    private final Cols cols;

    private static final class Cols {
        final String id, nickname, tier, amount, currency, deliveredAt, attempts, note, orderedAt;
        Cols(String id, String nickname, String tier, String amount, String currency,
             String deliveredAt, String attempts, String note, String orderedAt) {
            this.id = id;
            this.nickname = nickname;
            this.tier = tier;
            this.amount = amount;
            this.currency = currency;
            this.deliveredAt = deliveredAt;
            this.attempts = attempts;
            this.note = note;
            this.orderedAt = orderedAt;
        }
    }

    public HotMcVoteRewardSource(DbPool db, SafeConfig cfg, java.util.logging.Logger log) {
        this.db = db;
        this.log = log;

        this.tableName = cfg.getString("table.name", "external_data.vote_hotmc");
        this.whereReady = cfg.getString("table.readyWhere", "delivered_at IS NULL");
        this.orderBy = cfg.getString("table.orderBy", "voted_at ASC, id ASC");
        this.defaultTier = cfg.getString("defaultTier", "hotmc_vote");

        this.cols = new Cols(
                nullIfEmpty(cfg.getString("table.columns.id", "id")),
                nullIfEmpty(cfg.getString("table.columns.nickname", "nickname")),
                nullIfEmpty(cfg.getString("table.columns.tier", null)),
                nullIfEmpty(cfg.getString("table.columns.amount", null)),
                nullIfEmpty(cfg.getString("table.columns.currency", null)),
                nullIfEmpty(cfg.getString("table.columns.delivered_at", "delivered_at")),
                nullIfEmpty(cfg.getString("table.columns.delivery_attempts", "delivery_attempts")),
                nullIfEmpty(cfg.getString("table.columns.delivery_note", "delivery_note")),
                nullIfEmpty(cfg.getString("table.columns.ordered_at", "voted_at"))
        );
    }

    private static String nullIfEmpty(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    @Override
    public String name() {
        return "votes";
    }

    @Override
    public List<RewardItem> fetchPending(int batchSize) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ")
                .append(cols.id).append(" AS id, ")
                .append(cols.nickname).append(" AS nickname");

        if (cols.tier != null)      sb.append(", ").append(cols.tier).append(" AS tier");
        else                        sb.append(", NULL AS tier");

        if (cols.amount != null)    sb.append(", ").append(cols.amount).append(" AS amount");
        else                        sb.append(", 0 AS amount");

        if (cols.currency != null)  sb.append(", ").append(cols.currency).append(" AS currency");
        else                        sb.append(", NULL AS currency");

        if (cols.attempts != null)  sb.append(", ").append(cols.attempts).append(" AS attempts");
        else                        sb.append(", 0 AS attempts");

        if (cols.orderedAt != null) sb.append(", ").append(cols.orderedAt).append(" AS ordered_at");
        else                        sb.append(", NOW() AS ordered_at");

        sb.append(" FROM ").append(tableName)
                .append(" WHERE ").append(whereReady)
                .append(" ORDER BY ").append(orderBy)
                .append(" LIMIT ?");

        String sql = sb.toString();

        List<RewardItem> out = new ArrayList<>();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setInt(1, batchSize);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long id = rs.getLong("id");
                    String nick = rs.getString("nickname");

                    String tier = null;
                    try { tier = rs.getString("tier"); } catch (SQLException ignored) {}
                    if (tier == null || tier.isEmpty()) tier = defaultTier;

                    double amount = 0.0;
                    try {
                        amount = rs.getDouble("amount");
                        if (rs.wasNull()) amount = 0.0;
                    } catch (SQLException ignored) {}

                    String cur = null;
                    try { cur = rs.getString("currency"); } catch (SQLException ignored) {}

                    Map<String, Object> attrs = new HashMap<>();
                    try { attrs.put("attempts", rs.getInt("attempts")); } catch (SQLException ignored) {}
                    try { attrs.put("ordered_at", rs.getTimestamp("ordered_at")); } catch (SQLException ignored) {}

                    // orderId для логов / плейсхолдеров
                    out.add(new RewardItem(id, "vote#" + id, nick, tier, amount, cur, attrs));
                }
            }
        }
        return out;
    }

    @Override
    public boolean markDelivered(Connection txConn, long id) throws Exception {
        List<String> sets = new ArrayList<>();
        sets.add(cols.deliveredAt + " = NOW()");
        if (cols.attempts != null) sets.add(cols.attempts + " = " + cols.attempts + " + 1");
        if (cols.note != null)     sets.add(cols.note + " = 'ok'");

        String sql = "UPDATE " + tableName + " SET " + String.join(", ", sets) +
                " WHERE " + cols.id + " = ? AND " + cols.deliveredAt + " IS NULL";

        try (PreparedStatement ps = txConn.prepareStatement(sql)) {
            ps.setLong(1, id);
            return ps.executeUpdate() == 1;
        }
    }

    @Override
    public boolean markFailed(Connection txConn, long id, String reason) throws Exception {
        List<String> sets = new ArrayList<>();
        if (cols.attempts != null) sets.add(cols.attempts + " = " + cols.attempts + " + 1");
        if (cols.note != null)     sets.add(cols.note + " = ?");

        if (sets.isEmpty()) sets.add(cols.deliveredAt + " = " + cols.deliveredAt);

        String sql = "UPDATE " + tableName + " SET " + String.join(", ", sets) +
                " WHERE " + cols.id + " = ? AND " + cols.deliveredAt + " IS NULL";

        try (PreparedStatement ps = txConn.prepareStatement(sql)) {
            int idx = 1;
            if (cols.note != null) {
                ps.setString(idx++, trim(reason));
            }
            ps.setLong(idx, id);
            return ps.executeUpdate() == 1;
        }
    }

    private static String trim(String s) {
        if (s == null) return "error";
        s = s.replaceAll("[\\r\\n\\t]+", " ").trim();
        return s.length() > 240 ? s.substring(0, 240) : s;
    }
}
