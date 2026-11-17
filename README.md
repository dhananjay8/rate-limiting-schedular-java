# Rate-Limiting Schedular Service

A standalone Kafka-to-Kafka task scheduler that uses Redis and Lua for atomic FIFO queueing, per-queue in-flight limits, delayed execution, and acknowledgement-driven continuation.

The limit is a **maximum number of concurrent unacknowledged tasks per queue**, not a requests-per-second or token-bucket limit. Redis holds scheduler coordination state; task business state remains with the producer and worker applications.

## Capabilities

- Kafka command intake with manual offset acknowledgement after Redis accepts a command.
- Per-queue FIFO ordering and configurable maximum in-flight task count.
- Atomic queue promotion and reservation using Redis Lua scripts.
- Delayed tasks scheduled by epoch-millisecond due time.
- Redis Streams dispatch outbox with consumer groups, pending-entry reclamation, and retry-safe Kafka publication.
- Stable task IDs for active-queue deduplication and worker-side idempotency.
- Redis Cluster hash-slot-aware sharding, Actuator probes, Prometheus metrics, structured SLF4J logging, and graceful shutdown.

Delivery is **at least once** across Redis and Kafka. A process can stop after Kafka accepts a record but before the Redis stream entry is acknowledged; consumers must deduplicate on the `scheduler-task-id` Kafka header. Kafka producer idempotence reduces duplicates within a producer session but does not provide a cross-system transaction.

## Requirements

- Java 17+
- Gradle Wrapper (the first build downloads the pinned Gradle distribution)
- Redis 7.0+ (Lua scripts and Redis Streams; Redis Cluster supported)
- Apache Kafka 3.6+
- Podman and `podman-compose` for the local dependency stack

## Quick start

```bash
podman-compose up -d
./gradlew bootRun
```

The local Kafka broker is reachable at `localhost:9092`; Redis is at `localhost:6379`. The service listens on port `8080`. Readiness and liveness are exposed at `/actuator/health/readiness` and `/actuator/health/liveness`; Prometheus metrics are at `/actuator/prometheus`. Restrict actuator endpoints to trusted monitoring networks in deployed environments.

Override connection settings with environment variables:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
REDIS_URL=redis://localhost:6379 \
SCHEDULER_COMMAND_TOPIC=scheduler.commands.v1 \
./gradlew bootRun
```

For an OCI image, build with `podman build -f Containerfile -t rate-limiting-schedular-service:local .`.

## Command contract

Publish JSON records to the configured command topic (`scheduler.commands.v1` by default). Fields unknown to this version are ignored to allow additive schema evolution.

Enqueue immediately:

```json
{
  "type": "ENQUEUE",
  "queueId": "tenant-42:scan",
  "taskId": "task-1001",
  "topic": "worker.scan.v1",
  "payload": "{\"resourceId\":\"resource-9\"}",
  "maxInFlight": 5
}
```

Schedule a delayed task:

```json
{
  "type": "SCHEDULE",
  "queueId": "tenant-42:scan",
  "taskId": "task-1002",
  "topic": "worker.scan.v1",
  "payload": "{\"resourceId\":\"resource-10\"}",
  "maxInFlight": 5,
  "executeAtEpochMillis": 1893456000000
}
```

Release one in-flight slot after the worker finishes:

```json
{
  "type": "ACK",
  "queueId": "tenant-42:scan",
  "taskId": "task-1001"
}
```

`queueId` and `taskId` accept 1-128 ASCII letters, digits, dots, underscores, colons, or hyphens. `topic` accepts Kafka topic characters (`A-Z`, `a-z`, digits, `.`, `_`, `-`) up to 249 characters, excluding `.` and `..`. Payloads are opaque UTF-8 strings with a configurable size limit. Use a unique task ID per logical attempt and make the worker's completion handler idempotent.

The first accepted command for an active queue fixes its `maxInFlight` value and destination topic. Commands specifying a different value or topic for that queue are rejected to the dead-letter topic until the queue drains. Duplicate task IDs are ignored while active even after the deduplication TTL expires; after ACK, the TTL marker suppresses stale command redelivery. Once every pending, delayed, and in-flight task completes, the queue's configuration metadata is removed and the next command may set new values.

### Kafka dispatch

Dispatched payloads are sent to the queue's fixed topic with `queueId` as the Kafka key to preserve order within that queue. Producers should also use `queueId` as the Kafka key for commands. The service adds a `scheduler-task-id` header. Worker applications publish an `ACK` command only after their task reaches a terminal state; retry by scheduling a new task ID, then ACK the previous attempt.

## Configuration

All settings are externalized. Important environment overrides:

| Environment variable | Default | Description |
|---|---:|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap addresses |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL; use TLS/auth in deployed environments |
| `KAFKA_COMMAND_GROUP` | `scheduler-command-processors-v1` | Command consumer group |
| `SCHEDULER_COMMAND_TOPIC` | `scheduler.commands.v1` | Input command topic |
| `SCHEDULER_KEY_PREFIX` | `scheduler:v1` | Redis namespace; must not contain braces |
| `SCHEDULER_SHARD_COUNT` | `64` | Number of Redis hash-tag shards |
| `SCHEDULER_ACTIVE_QUEUE_BATCH_SIZE` | `500` | Active queue IDs examined per shard per poll |
| `SCHEDULER_PROMOTION_BATCH_SIZE` | `100` | Tasks moved per queue per poll |
| `SCHEDULER_DISPATCH_BATCH_SIZE` | `100` | Redis Stream records read per shard per poll |
| `SCHEDULER_MAX_PAYLOAD_BYTES` | `262144` | Maximum UTF-8 payload size |
| `SCHEDULER_DEDUPLICATION_TTL` | `7d` | Retention for enqueue deduplication keys |
| `SCHEDULER_DISPATCH_CLAIM_IDLE` | `30s` | Idle time before another replica reclaims a pending stream entry |
| `SCHEDULER_KAFKA_HEALTH_TIMEOUT` | `2s` | Kafka readiness probe timeout |
| `SCHEDULER_CONSUMER_ID` | hostname plus random ID | Redis Stream consumer identity |
| `SCHEDULER_QUEUE_POLL_INTERVAL` | `1000` | Queue scan delay in milliseconds |
| `SCHEDULER_DISPATCH_POLL_INTERVAL` | `250` | Kafka dispatch scan delay in milliseconds |

Create the command topic and its `.DLT` dead-letter topic before startup in production. Configure sufficient partitions for expected parallelism, and set replication/retention based on the deployment's recovery objectives. The local broker auto-creates topics for development only.

## Redis key lifecycle

Queue state and dispatch records are retained while work is active and removed after terminal acknowledgements. Live queue keys intentionally have no TTL: expiring them could silently discard work. Deduplication keys have a bounded TTL. Configure Redis persistence, replication, memory alerts, and `noeviction`; Redis is coordination state, not the system of record.

The namespace and shard count are part of the storage layout. Keep both stable across rolling deployments. Changing either requires a drain or a planned state migration. Lua scripts only touch keys with a shared Redis Cluster hash tag.

## Build and test

```bash
./gradlew clean check
./gradlew bootJar
scripts/test-redis-lua.sh
```

Unit tests validate command boundaries and key-slot construction. The Podman smoke test executes the Lua state transitions against an ephemeral Redis instance. Integration testing against Kafka should be run in CI or a local Podman environment before production rollout.

## Project layout

```text
src/main/java/.../kafka/       Kafka command listener and error handling
src/main/java/.../service/     Queue state, promotion, and dispatch
src/main/resources/redis/      Atomic Redis Lua scripts
src/test/java/                 Unit tests
docs/                          Architecture, event contract, and operations
compose.yaml                   Local Redis and Kafka dependencies
Containerfile                  OCI-compatible application image
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md), and [LICENSE](LICENSE). Changes to the Kafka command contract or Redis key layout require an explicit compatibility/migration note.
