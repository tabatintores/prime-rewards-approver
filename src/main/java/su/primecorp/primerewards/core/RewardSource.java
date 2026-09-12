package su.primecorp.primerewards.core;

import java.sql.Connection;
import java.util.List;

public interface RewardSource {
    String name();
    default String description() { return name(); }
    List<RewardItem> fetchPending(int batchSize) throws Exception;

    /** Старые источники поддерживают обновление по id. Для заказов нужен полный контекст. */
    default boolean markDelivered(Connection txConn, long id) throws Exception {
        throw new UnsupportedOperationException("Источник требует полный контекст заказа.");
    }

    default boolean markFailed(Connection txConn, long id, String reason) throws Exception {
        throw new UnsupportedOperationException("Источник требует полный контекст заказа.");
    }

    default boolean requiresClaim() { return false; }

    default boolean claim(Connection tx, RewardItem item, String token) throws Exception {
        throw new UnsupportedOperationException("Источник не поддерживает резервирование.");
    }

    default boolean markDelivered(Connection tx, RewardItem item, String token) throws Exception {
        return markDelivered(tx, item.id);
    }

    default boolean markFailed(Connection tx, RewardItem item, String token, String reason) throws Exception {
        return markFailed(tx, item.id, reason);
    }

    default String context(RewardItem item) {
        return name() + " id=" + item.id + " order_id=" + item.orderId;
    }
}
