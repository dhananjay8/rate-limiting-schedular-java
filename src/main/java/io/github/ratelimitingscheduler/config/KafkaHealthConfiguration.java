package io.github.ratelimitingscheduler.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Configuration
public class KafkaHealthConfiguration {

    @Bean(destroyMethod = "close")
    AdminClient schedulerKafkaAdminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    @Bean("kafkaHealthIndicator")
    HealthIndicator kafkaHealthIndicator(AdminClient schedulerKafkaAdminClient,
                                         SchedulerProperties properties) {
        return () -> {
            try {
                String clusterId = schedulerKafkaAdminClient.describeCluster().clusterId()
                        .get(properties.getKafkaHealthTimeout().toMillis(), TimeUnit.MILLISECONDS);
                return clusterId == null
                        ? Health.status(Status.DOWN).withDetail("reason", "Kafka returned no cluster ID").build()
                        : Health.up().withDetail("clusterId", clusterId).build();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return Health.down(exception).build();
            } catch (ExecutionException | TimeoutException exception) {
                return Health.down(exception).build();
            }
        };
    }
}
