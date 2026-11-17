package io.github.ratelimitingscheduler.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.ratelimitingscheduler.model.SchedulerCommand;
import io.github.ratelimitingscheduler.service.SchedulerCommandService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class SchedulerCommandListener {

    private final ObjectMapper objectMapper;
    private final SchedulerCommandService commandService;

    public SchedulerCommandListener(ObjectMapper objectMapper,
                                    SchedulerCommandService commandService) {
        this.objectMapper = objectMapper;
        this.commandService = commandService;
    }

    @KafkaListener(topics = "${scheduler.command-topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void receive(String message, Acknowledgment acknowledgment) {
        try {
            SchedulerCommand command = objectMapper.readValue(message, SchedulerCommand.class);
            commandService.process(command);
            acknowledgment.acknowledge();
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Invalid scheduler command JSON", exception);
        }
    }
}
