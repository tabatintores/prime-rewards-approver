package su.primecorp.primerewards.core;

import java.sql.Connection;
import java.util.List;

public interface RewardSource {
    String name();
    List<RewardItem> fetchPending(int batchSize) throws Exception;

    /** отметить успех — атомарно, только если delivered_at IS NULL */
    boolean markDelivered(Connection txConn, long id) throws Exception;

    /** отметить неуспех (без delivered_at), сохранить причину (<=255) */
    boolean markFailed(Connection txConn, long id, String reason) throws Exception;
}
