package io.github.ratelimitingscheduler.service;

import io.github.ratelimitingscheduler.config.SchedulerProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueueKeyspaceTest {

    @Test
    void queueStateAndShardInfrastructureShareTheSameRedisClusterSlot() {
        SchedulerProperties properties = new SchedulerProperties();
        QueueKeyspace keyspace = new QueueKeyspace(properties);
        String queueId = "tenant:region-1";
        String shardTag = "{s" + keyspace.shardFor(queueId) + "}";

        assertThat(keyspace.pending(queueId)).contains(shardTag);
        assertThat(keyspace.inFlight(queueId)).contains(shardTag);
        assertThat(keyspace.delayed(queueId)).contains(shardTag);
        assertThat(keyspace.queueLimit(queueId)).contains(shardTag);
        assertThat(keyspace.queueTopic(queueId)).contains(shardTag);
        assertThat(keyspace.taskIds(queueId)).contains(shardTag);
        assertThat(keyspace.activeQueues(keyspace.shardFor(queueId))).contains(shardTag);
        assertThat(keyspace.dispatchStream(keyspace.shardFor(queueId))).contains(shardTag);
    }

    @Test
    void queueIdentifiersAreEncodedBeforeBeingUsedInRedisKeys() {
        SchedulerProperties properties = new SchedulerProperties();
        QueueKeyspace keyspace = new QueueKeyspace(properties);

        assertThat(keyspace.pending("team/one"))
                .doesNotContain("team/one")
                .startsWith(properties.getKeyPrefix());
    }
}
