package io.github.ratelimitingscheduler.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

@Configuration
public class RedisScriptConfiguration {

    @Bean
    RedisScript<Long> enqueueTaskScript() {
        return longScript("redis/enqueue-task.lua");
    }

    @Bean
    RedisScript<Long> scheduleDelayedTaskScript() {
        return longScript("redis/schedule-delayed-task.lua");
    }

    @Bean
    RedisScript<Long> promoteDelayedTasksScript() {
        return longScript("redis/promote-delayed-tasks.lua");
    }

    @Bean
    RedisScript<Long> promoteQueueScript() {
        return longScript("redis/promote-queue.lua");
    }

    @Bean
    RedisScript<Long> acknowledgeTaskScript() {
        return longScript("redis/acknowledge-task.lua");
    }

    @Bean
    RedisScript<Long> ensureDispatchGroupScript() {
        return longScript("redis/ensure-dispatch-group.lua");
    }

    @Bean
    RedisScript<Long> acknowledgeDispatchScript() {
        return longScript("redis/acknowledge-dispatch.lua");
    }

    @Bean
    RedisScript<Long> renewDispatchLockScript() {
        return longScript("redis/renew-dispatch-lock.lua");
    }

    @Bean
    RedisScript<Long> releaseDispatchLockScript() {
        return longScript("redis/release-dispatch-lock.lua");
    }

    private RedisScript<Long> longScript(String path) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(path));
        script.setResultType(Long.class);
        return script;
    }
}
