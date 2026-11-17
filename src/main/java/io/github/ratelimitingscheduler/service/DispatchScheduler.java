package io.github.ratelimitingscheduler.service;

import io.github.ratelimitingscheduler.config.SchedulerProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DispatchScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DispatchScheduler.class);
    private static final Duration MINIMUM_DISPATCH_LOCK_TTL = Duration.ofSeconds(30);

    private final StringRedisTemplate redis;
    private final KafkaTemplate<String, String> kafka;
    private final QueueKeyspace keyspace;
    private final SchedulerProperties properties;
    private final RedisScript<Long> ensureDispatchGroupScript;
    private final RedisScript<Long> acknowledgeDispatchScript;
    private final RedisScript<Long> renewDispatchLockScript;
    private final RedisScript<Long> releaseDispatchLockScript;
    private final MeterRegistry metrics;
    private final Set<Integer> initializedShards = ConcurrentHashMap.newKeySet();

    public DispatchScheduler(
            StringRedisTemplate redis,
            KafkaTemplate<String, String> kafka,
            QueueKeyspace keyspace,
            SchedulerProperties properties,
            @Qualifier("ensureDispatchGroupScript") RedisScript<Long> ensureDispatchGroupScript,
            @Qualifier("acknowledgeDispatchScript") RedisScript<Long> acknowledgeDispatchScript,
            @Qualifier("renewDispatchLockScript") RedisScript<Long> renewDispatchLockScript,
            @Qualifier("releaseDispatchLockScript") RedisScript<Long> releaseDispatchLockScript,
            MeterRegistry metrics) {
        this.redis = redis;
        this.kafka = kafka;
        this.keyspace = keyspace;
        this.properties = properties;
        this.ensureDispatchGroupScript = ensureDispatchGroupScript;
        this.acknowledgeDispatchScript = acknowledgeDispatchScript;
        this.renewDispatchLockScript = renewDispatchLockScript;
        this.releaseDispatchLockScript = releaseDispatchLockScript;
        this.metrics = metrics;
    }

    /**
     * Serializes each shard's publish order across replicas with a bounded Redis lease.
     */
    @Scheduled(fixedDelayString = "${scheduler.dispatch-poll-interval:250}")
    public void dispatch() {
        try {
            for (int shard = 0; shard < properties.getShardCount(); shard++) {
                dispatchShard(shard);
            }
        } catch (RuntimeException exception) {
            LOGGER.error("Kafka dispatch poll failed", exception);
            metrics.counter("scheduler.dispatch.failures").increment();
        }
    }

    private void dispatchShard(int shard) {
        String lockKey = keyspace.dispatchLock(shard);
        String lockToken = UUID.randomUUID().toString();
        Duration lockTtl = properties.getKafkaSendTimeout().multipliedBy(3)
                .compareTo(MINIMUM_DISPATCH_LOCK_TTL) < 0
                ? MINIMUM_DISPATCH_LOCK_TTL
                : properties.getKafkaSendTimeout().multipliedBy(3);
        Boolean acquired = redis.opsForValue().setIfAbsent(lockKey, lockToken, lockTtl);
        if (!Boolean.TRUE.equals(acquired)) {
            return;
        }

        try {
            dispatchShardLocked(shard, lockKey, lockToken, lockTtl);
        } finally {
            try {
                redis.execute(releaseDispatchLockScript, List.of(lockKey), lockToken);
            } catch (RuntimeException exception) {
                LOGGER.warn("Could not release dispatch lock for shard {}", shard, exception);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void dispatchShardLocked(int shard, String lockKey, String lockToken, Duration lockTtl) {
        String stream = keyspace.dispatchStream(shard);
        ensureConsumerGroup(shard, stream);
        StreamOperations<String, String, String> streams = redis.opsForStream();
        Consumer consumer = Consumer.from(properties.getDispatchGroup(), properties.getConsumerId());

        PendingMessages pending = streams.pending(
                stream, properties.getDispatchGroup(), Range.unbounded(),
                properties.getDispatchBatchSize());
        if (!pending.isEmpty()) {
            List<RecordId> reclaimIds = reclaimableRecords(pending);
            if (reclaimIds.isEmpty()) {
                return;
            }
            List<MapRecord<String, String, String>> claimed = streams.claim(
                    stream, properties.getDispatchGroup(), consumer.getName(),
                    properties.getDispatchClaimIdle(), reclaimIds.toArray(RecordId[]::new));
            if (!claimed.isEmpty()) {
                dispatchRecords(stream, claimed, lockKey, lockToken, lockTtl);
            }
            return;
        }

        List<MapRecord<String, String, String>> records = streams.read(
                consumer,
                StreamReadOptions.empty().count(properties.getDispatchBatchSize()),
                StreamOffset.<String>create(stream, ReadOffset.lastConsumed()));
        if (records != null && !records.isEmpty()) {
            dispatchRecords(stream, records, lockKey, lockToken, lockTtl);
        }
    }

    private List<RecordId> reclaimableRecords(PendingMessages pending) {
        List<RecordId> reclaimable = new ArrayList<>();
        for (PendingMessage message : pending) {
            if (message.getElapsedTimeSinceLastDelivery().compareTo(properties.getDispatchClaimIdle()) < 0) {
                break;
            }
            reclaimable.add(message.getId());
        }
        return reclaimable;
    }

    private void dispatchRecords(String stream, List<MapRecord<String, String, String>> records,
                                 String lockKey, String lockToken, Duration lockTtl) {
        List<ProducerRecord<String, String>> kafkaRecords = new ArrayList<>(records.size());
        for (MapRecord<String, String, String> record : records) {
            Map<String, String> fields = record.getValue();
            String queueId = required(fields, "queueId");
            String taskId = required(fields, "taskId");
            String topic = required(fields, "topic");
            String payload = required(fields, "payload");

            ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(topic, queueId, payload);
            kafkaRecord.headers().add(new RecordHeader("scheduler-task-id",
                    taskId.getBytes(StandardCharsets.UTF_8)));
            kafkaRecords.add(kafkaRecord);
        }

        Long renewed = redis.execute(renewDispatchLockScript, List.of(lockKey),
                lockToken, Long.toString(lockTtl.toMillis()));
        if (renewed == null || renewed != 1) {
            throw new IllegalStateException("Dispatch lock ownership was lost");
        }

        try {
            List<CompletableFuture<?>> sends = new ArrayList<>(kafkaRecords.size());
            for (ProducerRecord<String, String> kafkaRecord : kafkaRecords) {
                sends.add(kafka.send(kafkaRecord));
            }
            CompletableFuture.allOf(sends.toArray(CompletableFuture<?>[]::new))
                    .get(properties.getKafkaSendTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Kafka dispatch interrupted for shard stream {}", stream, exception);
            metrics.counter("scheduler.dispatch.failures").increment();
            return;
        } catch (Exception exception) {
            LOGGER.warn("Kafka dispatch failed for shard stream {}", stream, exception);
            metrics.counter("scheduler.dispatch.failures").increment();
            return;
        }

        for (MapRecord<String, String, String> record : records) {
            Long acknowledged = redis.execute(acknowledgeDispatchScript, List.of(stream),
                    properties.getDispatchGroup(), record.getId().getValue());
            if (acknowledged == null || acknowledged != 1) {
                throw new IllegalStateException("Kafka send succeeded but Redis dispatch acknowledgement failed");
            }
            metrics.counter("scheduler.dispatch.sent").increment();
        }
    }

    private void ensureConsumerGroup(int shard, String stream) {
        if (initializedShards.contains(shard)) {
            return;
        }
        Long result = redis.execute(ensureDispatchGroupScript, List.of(stream),
                properties.getDispatchGroup());
        if (result == null) {
            throw new IllegalStateException("Redis did not initialize the dispatch consumer group");
        }
        initializedShards.add(shard);
    }

    private String required(Map<String, String> fields, String field) {
        String value = fields.get(field);
        if (value == null) {
            throw new IllegalStateException("Redis dispatch record is missing " + field);
        }
        return value;
    }
}
