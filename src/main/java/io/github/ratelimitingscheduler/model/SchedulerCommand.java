package io.github.ratelimitingscheduler.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Version-one command contract consumed from the configured Kafka command topic.
 * Fields are conditionally required based on {@code type}; validation is performed
 * before any Redis state is changed.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SchedulerCommand(
        CommandType type,
        String queueId,
        String taskId,
        String topic,
        String payload,
        Integer maxInFlight,
        Long executeAtEpochMillis) {
}
