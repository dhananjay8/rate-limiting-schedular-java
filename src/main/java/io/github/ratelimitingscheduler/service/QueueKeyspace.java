package io.github.ratelimitingscheduler.service;

import io.github.ratelimitingscheduler.config.SchedulerProperties;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Creates namespaced, Redis-Cluster-safe keys. Queue state and the shard's
 * dispatch stream share a hash tag so each Lua transaction stays in one slot.
 */
@Component
public class QueueKeyspace {

    private final SchedulerProperties properties;

    public QueueKeyspace(SchedulerProperties properties) {
        this.properties = properties;
    }

    public int shardFor(String queueId) {
        return Math.floorMod(queueId.hashCode(), properties.getShardCount());
    }

    public String activeQueues(int shard) {
        return shardTag(shard) + ":active";
    }

    public String dispatchStream(int shard) {
        return shardTag(shard) + ":dispatch";
    }

    public String dispatchLock(int shard) {
        return shardTag(shard) + ":dispatch-lock";
    }

    public String pending(String queueId) {
        return queueTag(queueId) + ":pending";
    }

    public String inFlight(String queueId) {
        return queueTag(queueId) + ":inflight";
    }

    public String delayed(String queueId) {
        return queueTag(queueId) + ":delayed";
    }

    public String queueLimit(String queueId) {
        return queueTag(queueId) + ":limit";
    }

    public String queueTopic(String queueId) {
        return queueTag(queueId) + ":topic";
    }

    public String taskIds(String queueId) {
        return queueTag(queueId) + ":tasks";
    }

    public String deduplication(String queueId, String taskId) {
        return queueTag(queueId) + ":dedupe:" + encode(taskId);
    }

    private String queueTag(String queueId) {
        return shardTag(shardFor(queueId)) + ":q:" + encode(queueId);
    }

    private String shardTag(int shard) {
        return properties.getKeyPrefix() + ":{s" + shard + "}";
    }

    private String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
