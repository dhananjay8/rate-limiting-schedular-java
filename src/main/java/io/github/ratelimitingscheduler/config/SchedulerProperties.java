package io.github.ratelimitingscheduler.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {

    @NotBlank
    @Pattern(regexp = "[A-Za-z0-9._:-]+")
    private String keyPrefix = "scheduler:v1";

    @Min(1)
    @Max(4096)
    private int shardCount = 64;

    @Min(1)
    private int activeQueueBatchSize = 500;

    @Min(1)
    private int promotionBatchSize = 100;

    @Min(1)
    private int dispatchBatchSize = 100;

    @Min(1)
    private int maxPayloadBytes = 262144;

    @NotNull
    private Duration deduplicationTtl = Duration.ofDays(7);

    @NotNull
    private Duration dispatchClaimIdle = Duration.ofSeconds(30);

    @NotNull
    private Duration kafkaSendTimeout = Duration.ofSeconds(10);

    @NotNull
    private Duration kafkaHealthTimeout = Duration.ofSeconds(2);

    @Min(1)
    private long queuePollInterval = 1000;

    @Min(1)
    private long dispatchPollInterval = 250;

    @NotBlank
    private String commandTopic = "scheduler.commands.v1";

    @NotBlank
    private String deadLetterSuffix = ".DLT";

    @NotBlank
    private String dispatchGroup = "scheduler-dispatchers-v1";

    @NotBlank
    private String consumerId = "local";

    @AssertTrue(message = "deduplication-ttl must be at least one second")
    public boolean isDeduplicationTtlPositive() {
        return deduplicationTtl != null && deduplicationTtl.getSeconds() >= 1;
    }

    @AssertTrue(message = "dispatch-claim-idle must exceed kafka-send-timeout")
    public boolean isDispatchClaimIdleSafe() {
        return dispatchClaimIdle != null && kafkaSendTimeout != null
                && dispatchClaimIdle.compareTo(kafkaSendTimeout) > 0;
    }

    public Duration getKafkaHealthTimeout() {
        return kafkaHealthTimeout;
    }

    public void setKafkaHealthTimeout(Duration kafkaHealthTimeout) {
        this.kafkaHealthTimeout = kafkaHealthTimeout;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public int getShardCount() {
        return shardCount;
    }

    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    public int getActiveQueueBatchSize() {
        return activeQueueBatchSize;
    }

    public void setActiveQueueBatchSize(int activeQueueBatchSize) {
        this.activeQueueBatchSize = activeQueueBatchSize;
    }

    public int getPromotionBatchSize() {
        return promotionBatchSize;
    }

    public void setPromotionBatchSize(int promotionBatchSize) {
        this.promotionBatchSize = promotionBatchSize;
    }

    public int getDispatchBatchSize() {
        return dispatchBatchSize;
    }

    public void setDispatchBatchSize(int dispatchBatchSize) {
        this.dispatchBatchSize = dispatchBatchSize;
    }

    public int getMaxPayloadBytes() {
        return maxPayloadBytes;
    }

    public void setMaxPayloadBytes(int maxPayloadBytes) {
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public Duration getDeduplicationTtl() {
        return deduplicationTtl;
    }

    public void setDeduplicationTtl(Duration deduplicationTtl) {
        this.deduplicationTtl = deduplicationTtl;
    }

    public Duration getDispatchClaimIdle() {
        return dispatchClaimIdle;
    }

    public void setDispatchClaimIdle(Duration dispatchClaimIdle) {
        this.dispatchClaimIdle = dispatchClaimIdle;
    }

    public Duration getKafkaSendTimeout() {
        return kafkaSendTimeout;
    }

    public void setKafkaSendTimeout(Duration kafkaSendTimeout) {
        this.kafkaSendTimeout = kafkaSendTimeout;
    }

    public long getQueuePollInterval() {
        return queuePollInterval;
    }

    public void setQueuePollInterval(long queuePollInterval) {
        this.queuePollInterval = queuePollInterval;
    }

    public long getDispatchPollInterval() {
        return dispatchPollInterval;
    }

    public void setDispatchPollInterval(long dispatchPollInterval) {
        this.dispatchPollInterval = dispatchPollInterval;
    }

    public String getCommandTopic() {
        return commandTopic;
    }

    public void setCommandTopic(String commandTopic) {
        this.commandTopic = commandTopic;
    }

    public String getDeadLetterSuffix() {
        return deadLetterSuffix;
    }

    public void setDeadLetterSuffix(String deadLetterSuffix) {
        this.deadLetterSuffix = deadLetterSuffix;
    }

    public String getDispatchGroup() {
        return dispatchGroup;
    }

    public void setDispatchGroup(String dispatchGroup) {
        this.dispatchGroup = dispatchGroup;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }
}
