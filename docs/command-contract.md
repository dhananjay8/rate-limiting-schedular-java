# Kafka Command Contract

The command topic name is configured by `SCHEDULER_COMMAND_TOPIC`. The current contract is JSON and supports additive unknown fields. See [command-schema.json](command-schema.json). Producers should use `queueId` as the Kafka record key to keep commands for a queue in one partition and send UTF-8 values. Reuse a stable task ID for retries of the same command.

## Common constraints

- `queueId`, `taskId`: 1-128 characters matching `[A-Za-z0-9._:-]`.
- `topic`: Kafka topic identifier using letters, digits, `.`, `_`, or `-`, up to 249 characters, except `.` and `..`.
- `payload`: opaque UTF-8 text; default maximum size is 256 KiB.
- `maxInFlight`: positive integer, fixed with the destination topic for the active queue until it drains.
- Command processing is at least once. Duplicate task IDs are ignored while active and for the configured deduplication TTL after completion.

## `ENQUEUE`

Required: `type`, `queueId`, `taskId`, `topic`, `payload`, `maxInFlight`.

Appends a task to the queue's FIFO tail. The task becomes eligible for dispatch when the number of in-flight tasks falls below the queue limit.

## `SCHEDULE`

Required: all `ENQUEUE` fields plus `executeAtEpochMillis`.

Stores a delayed task in a Redis sorted set. It is moved into the FIFO queue when its due time is reached. Polling interval bounds normal due-time latency; this is not a real-time timer.

## `ACK`

Required: `type`, `queueId`, `taskId`.

Releases the reservation held for a delivered task. Repeated acknowledgements are harmless. ACK after completion or terminal failure; when retrying, schedule a new task ID and ACK the previous attempt.

## Dispatch record

The original payload is sent to the task's `topic`; the Kafka record key is `queueId`. The service adds the `scheduler-task-id` header. Workers should make side effects idempotent with that ID and publish an `ACK` command after terminal handling.

## Compatibility

Additive fields are ignored by older consumers. Do not change meanings, required fields, or enum values without introducing a compatible versioned topic/contract and a migration plan.
