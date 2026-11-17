package io.github.ratelimitingscheduler.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.ratelimitingscheduler.config.SchedulerProperties;
import io.github.ratelimitingscheduler.model.CommandType;
import io.github.ratelimitingscheduler.model.SchedulerCommand;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SchedulerCommandServiceTest {

    private final QueueStore queueStore = mock(QueueStore.class);
    private final SchedulerCommandService service = new SchedulerCommandService(
            queueStore, new ObjectMapper(), new SchedulerProperties(), new SimpleMeterRegistry());

    @Test
    void enqueueRequiresAValidConcurrencyLimitAndPayload() {
        SchedulerCommand command = new SchedulerCommand(
                CommandType.ENQUEUE, "account-1", "task-1", "worker.events",
                null, 3, null);

        assertThatThrownBy(() -> service.process(command))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("payload is required");
    }

    @Test
    void acknowledgementOnlyRequiresQueueAndTaskIdentifiers() {
        SchedulerCommand command = new SchedulerCommand(
                CommandType.ACK, "account-1", "task-1", null, null, null, null);

        service.process(command);

        verify(queueStore).acknowledge("account-1", "task-1");
    }

    @Test
    void acknowledgementRejectsTaskFieldsThatDoNotApply() {
        SchedulerCommand command = new SchedulerCommand(
                CommandType.ACK, "account-1", "task-1", "worker.events", null, null, null);

        assertThatThrownBy(() -> service.process(command))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("ACK accepts only type, queueId, and taskId");
    }
}
