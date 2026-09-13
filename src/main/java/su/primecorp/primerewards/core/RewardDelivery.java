package su.primecorp.primerewards.core;

/** Граница между асинхронным диспетчером и исполнителем игровых действий. */
public interface RewardDelivery {
    RewardExecutor.PreparedReward prepare(RewardItem item, String sourceName) throws Exception;
    void execute(RewardExecutor.PreparedReward reward) throws Exception;
    void stop();
}
