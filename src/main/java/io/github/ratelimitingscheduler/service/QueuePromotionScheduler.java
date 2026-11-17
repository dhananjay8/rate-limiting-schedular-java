package io.github.ratelimitingscheduler.service;

import io.github.ratelimitingscheduler.config.SchedulerProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class QueuePromotionScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueuePromotionScheduler.class);

    private final QueueStore queueStore;
    private final QueueKeyspace keyspace;
    private final SchedulerProperties properties;
    private final MeterRegistry metrics;

    public QueuePromotionScheduler(QueueStore queueStore,
                                   QueueKeyspace keyspace,
                                   SchedulerProperties properties,
                                   MeterRegistry metrics) {
        this.queueStore = queueStore;
        this.keyspace = keyspace;
        this.properties = properties;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${scheduler.queue-poll-interval:1000}")
    public void promoteQueues() {
        long now = Instant.now().toEpochMilli();
        try {
            for (int shard = 0; shard < properties.getShardCount(); shard++) {
                for (String queueId : queueStore.dueQueues(shard, now)) {
                    queueStore.promoteDelayed(queueId, now);
                    long promoted = queueStore.promote(queueId, now);
                    if (promoted > 0) {
                        metrics.counter("scheduler.tasks.promoted").increment(promoted);
                    }
                }
            }
        } catch (RuntimeException exception) {
            LOGGER.error("Queue promotion failed", exception);
            metrics.counter("scheduler.queue-promotion.failures").increment();
        }
    }
}
