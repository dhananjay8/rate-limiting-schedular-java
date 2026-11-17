package io.github.ratelimitingscheduler.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.ratelimitingscheduler.config.SchedulerProperties;
import io.github.ratelimitingscheduler.model.CommandType;
import io.github.ratelimitingscheduler.model.SchedulerCommand;
import io.github.ratelimitingscheduler.model.TaskEnvelope;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.regex.Pattern;

@Service
public class SchedulerCommandService {

    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z0-9._:-]{1,128}");
    private static final Pattern SAFE_TOPIC = Pattern.compile("[A-Za-z0-9._-]{1,249}");

    private final QueueStore queueStore;
    private final ObjectMapper objectMapper;
    private final SchedulerProperties properties;
    private final MeterRegistry metrics;

    public SchedulerCommandService(QueueStore queueStore,
                                   ObjectMapper objectMapper,
                                   SchedulerProperties properties,
                                   MeterRegistry metrics) {
        this.queueStore = queueStore;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.metrics = metrics;
    }

    /**
     * Validates a command before touching Redis so malformed input is retried then dead-lettered.
     */
    public void process(SchedulerCommand command) {
        validate(command);
        switch (command.type()) {
            case ENQUEUE -> enqueue(command);
            case SCHEDULE -> schedule(command);
            case ACK -> acknowledge(command);
        }
        metrics.counter("scheduler.commands.processed", "type", command.type().name()).increment();
    }

    private void enqueue(SchedulerCommand command) {
        TaskEnvelope task = task(command);
        boolean inserted = queueStore.enqueue(command.queueId(), task.taskId(),
                serialize(task), task.topic(), command.maxInFlight());
        if (inserted) {
            metrics.counter("scheduler.tasks.enqueued").increment();
        } else {
            metrics.counter("scheduler.commands.duplicate").increment();
        }
    }

    private void schedule(SchedulerCommand command) {
        TaskEnvelope task = task(command);
        boolean inserted = queueStore.schedule(command.queueId(), task.taskId(),
                serialize(task), task.topic(), command.maxInFlight(), command.executeAtEpochMillis());
        if (inserted) {
            metrics.counter("scheduler.tasks.delayed").increment();
        } else {
            metrics.counter("scheduler.commands.duplicate").increment();
        }
    }

    private void acknowledge(SchedulerCommand command) {
        if (queueStore.acknowledge(command.queueId(), command.taskId())) {
            metrics.counter("scheduler.tasks.acknowledged").increment();
        } else {
            metrics.counter("scheduler.acknowledgements.duplicate").increment();
        }
    }

    private TaskEnvelope task(SchedulerCommand command) {
        return new TaskEnvelope(command.taskId(), command.topic(), command.payload());
    }

    private String serialize(TaskEnvelope task) {
        try {
            return objectMapper.writeValueAsString(task);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Task payload could not be serialized", exception);
        }
    }

    private void validate(SchedulerCommand command) {
        if (command == null || command.type() == null) {
            throw new IllegalArgumentException("type is required");
        }
        requireIdentifier("queueId", command.queueId());
        requireIdentifier("taskId", command.taskId());

        if (command.type() == CommandType.ACK) {
            if (command.topic() != null || command.payload() != null
                    || command.maxInFlight() != null || command.executeAtEpochMillis() != null) {
                throw new IllegalArgumentException("ACK accepts only type, queueId, and taskId");
            }
            return;
        }

        requireTopic(command.topic());
        if (command.payload() == null) {
            throw new IllegalArgumentException("payload is required");
        }
        if (command.payload().getBytes(StandardCharsets.UTF_8).length > properties.getMaxPayloadBytes()) {
            throw new IllegalArgumentException("payload exceeds scheduler.max-payload-bytes");
        }
        if (command.maxInFlight() == null || command.maxInFlight() < 1) {
            throw new IllegalArgumentException("maxInFlight must be greater than zero");
        }

        if (command.type() == CommandType.SCHEDULE) {
            if (command.executeAtEpochMillis() == null
                    || command.executeAtEpochMillis() < Instant.now().toEpochMilli()) {
                throw new IllegalArgumentException("executeAtEpochMillis must be in the future");
            }
        } else if (command.executeAtEpochMillis() != null) {
            throw new IllegalArgumentException("executeAtEpochMillis is only valid for SCHEDULE");
        }
    }

    private void requireIdentifier(String field, String value) {
        if (value == null || !SAFE_IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " must match " + SAFE_IDENTIFIER.pattern());
        }
    }

    private void requireTopic(String topic) {
        if (topic == null || !SAFE_TOPIC.matcher(topic).matches()
                || topic.equals(".") || topic.equals("..")) {
            throw new IllegalArgumentException("topic must be a valid Kafka topic name");
        }
    }
}
