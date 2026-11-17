package io.github.ratelimitingscheduler.service;

import io.github.ratelimitingscheduler.config.SchedulerProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Set;

@Repository
public class QueueStore {

    private final StringRedisTemplate redis;
    private final QueueKeyspace keyspace;
    private final SchedulerProperties properties;
    private final RedisScript<Long> enqueueTaskScript;
    private final RedisScript<Long> scheduleDelayedTaskScript;
    private final RedisScript<Long> promoteDelayedTasksScript;
    private final RedisScript<Long> promoteQueueScript;
    private final RedisScript<Long> acknowledgeTaskScript;

    public QueueStore(
            StringRedisTemplate redis,
            QueueKeyspace keyspace,
            SchedulerProperties properties,
            @Qualifier("enqueueTaskScript") RedisScript<Long> enqueueTaskScript,
            @Qualifier("scheduleDelayedTaskScript") RedisScript<Long> scheduleDelayedTaskScript,
            @Qualifier("promoteDelayedTasksScript") RedisScript<Long> promoteDelayedTasksScript,
            @Qualifier("promoteQueueScript") RedisScript<Long> promoteQueueScript,
            @Qualifier("acknowledgeTaskScript") RedisScript<Long> acknowledgeTaskScript) {
        this.redis = redis;
        this.keyspace = keyspace;
        this.properties = properties;
        this.enqueueTaskScript = enqueueTaskScript;
        this.scheduleDelayedTaskScript = scheduleDelayedTaskScript;
        this.promoteDelayedTasksScript = promoteDelayedTasksScript;
        this.promoteQueueScript = promoteQueueScript;
        this.acknowledgeTaskScript = acknowledgeTaskScript;
    }

    /**
     * Appends a deduplicated task and activates its queue in one Redis transaction.
     */
    public boolean enqueue(String queueId, String taskId, String taskJson,
                           String topic, int maxInFlight) {
        Long result = redis.execute(enqueueTaskScript, List.of(
                        keyspace.pending(queueId),
                        keyspace.activeQueues(keyspace.shardFor(queueId)),
                        keyspace.deduplication(queueId, taskId),
                        keyspace.queueLimit(queueId),
                        keyspace.queueTopic(queueId),
                        keyspace.taskIds(queueId)),
                queueId, taskId, taskJson, Integer.toString(maxInFlight),
                Long.toString(properties.getDeduplicationTtl().toSeconds()), topic);
        return result != null && result == 1;
    }

    /**
     * Adds a task to its due-time sorted set without losing queue activation state.
     */
    public boolean schedule(String queueId, String taskId, String taskJson, String topic,
                            int maxInFlight, long executeAtEpochMillis) {
        Long result = redis.execute(scheduleDelayedTaskScript, List.of(
                        keyspace.delayed(queueId),
                        keyspace.activeQueues(keyspace.shardFor(queueId)),
                        keyspace.deduplication(queueId, taskId),
                        keyspace.queueLimit(queueId),
                        keyspace.queueTopic(queueId),
                        keyspace.taskIds(queueId)),
                queueId, taskId, taskJson, Integer.toString(maxInFlight),
                Long.toString(executeAtEpochMillis),
                Long.toString(properties.getDeduplicationTtl().toSeconds()), topic);
        return result != null && result == 1;
    }

    /**
     * Releases the task reservation and wakes pending work when capacity becomes available.
     */
    public boolean acknowledge(String queueId, String taskId) {
        Long result = redis.execute(acknowledgeTaskScript, List.of(
                        keyspace.pending(queueId),
                        keyspace.inFlight(queueId),
                        keyspace.delayed(queueId),
                        keyspace.activeQueues(keyspace.shardFor(queueId)),
                        keyspace.queueLimit(queueId),
                        keyspace.queueTopic(queueId),
                        keyspace.taskIds(queueId)),
                queueId, taskId);
        return result != null && result == 1;
    }

    public Set<String> dueQueues(int shard, long nowEpochMillis) {
        Set<String> queues = redis.opsForZSet().rangeByScore(
                keyspace.activeQueues(shard),
                0,
                nowEpochMillis,
                0,
                properties.getActiveQueueBatchSize());
        return queues == null ? Set.of() : queues;
    }

    public long promoteDelayed(String queueId, long nowEpochMillis) {
        Long result = redis.execute(promoteDelayedTasksScript, List.of(
                        keyspace.pending(queueId),
                        keyspace.delayed(queueId),
                        keyspace.activeQueues(keyspace.shardFor(queueId))),
                queueId, Long.toString(nowEpochMillis),
                Integer.toString(properties.getPromotionBatchSize()));
        return result == null ? 0 : result;
    }

    /**
     * Reserves available queue capacity and appends dispatch records atomically.
     */
    public long promote(String queueId, long nowEpochMillis) {
        int shard = keyspace.shardFor(queueId);
        Long result = redis.execute(promoteQueueScript, List.of(
                        keyspace.pending(queueId),
                keyspace.inFlight(queueId),
                keyspace.dispatchStream(shard),
                keyspace.activeQueues(shard),
                keyspace.delayed(queueId),
                keyspace.queueLimit(queueId)),
                queueId, Integer.toString(properties.getPromotionBatchSize()),
                Long.toString(nowEpochMillis));
        return result == null ? 0 : result;
    }
}
