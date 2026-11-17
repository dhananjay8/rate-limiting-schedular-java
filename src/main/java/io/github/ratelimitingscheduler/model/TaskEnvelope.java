package io.github.ratelimitingscheduler.model;

/**
 * Internal representation retained in Redis until the worker acknowledges completion.
 */
public record TaskEnvelope(String taskId, String topic, String payload) {
}
