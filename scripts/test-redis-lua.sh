#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container_name="scheduler-redis-test-$$"
redis_image="${REDIS_TEST_IMAGE:-docker.io/library/redis:7-alpine}"

if ! command -v podman >/dev/null 2>&1; then
    echo "podman is required to run the Redis Lua integration checks" >&2
    exit 1
fi

podman run --rm --name "$container_name" -d "$redis_image" \
    redis-server --save '' --appendonly no --maxmemory-policy noeviction >/dev/null
trap 'podman stop "$container_name" >/dev/null 2>&1 || true' EXIT

ready=false
for _ in $(seq 1 30); do
    if podman exec "$container_name" redis-cli ping 2>/dev/null | grep -q PONG; then
        ready=true
        break
    fi
    sleep 1
done
if [[ "$ready" != true ]]; then
    echo "Redis test container did not become ready" >&2
    exit 1
fi

for script in "$root_dir"/src/main/resources/redis/*.lua; do
    podman cp "$script" "$container_name:/tmp/$(basename "$script")" >/dev/null
done

redis() {
    podman exec "$container_name" redis-cli --raw "$@"
}

eval_script() {
    local script="$1"
    shift
    podman exec "$container_name" redis-cli --raw --eval "/tmp/$script" "$@"
}

now_ms() {
    echo "$(( $(date +%s) * 1000 ))"
}

assert_equals() {
    local description="$1"
    local actual="$2"
    local expected="$3"
    if [[ "$actual" != "$expected" ]]; then
        echo "$description: expected '$expected', got '$actual'" >&2
        exit 1
    fi
}

assert_contains() {
    local description="$1"
    local actual="$2"
    local expected="$3"
    if [[ "$actual" != *"$expected"* ]]; then
        echo "$description: expected '$actual' to contain '$expected'" >&2
        exit 1
    fi
}

base='scheduler:test:{s0}'
active="$base:active"
stream="$base:dispatch"
queue='tenant-42:scan'
queue_key="$base:q:dGVuYW50LTQyOnNjYW4"
pending="$queue_key:pending"
inflight="$queue_key:inflight"
delayed="$queue_key:delayed"
limit="$queue_key:limit"
topic_key="$queue_key:topic"
task_ids="$queue_key:tasks"
dedupe_one="$queue_key:dedupe:dGFzay0x"
dedupe_two="$queue_key:dedupe:dGFzay0y"

assert_equals 'enqueue first task' \
    "$(eval_script enqueue-task.lua "$pending" "$active" "$dedupe_one" "$limit" "$topic_key" "$task_ids" , \
        "$queue" task-1 '{"taskId":"task-1","topic":"worker.events","payload":"one"}' \
        1 604800 worker.events)" 1
assert_equals 'enqueue second task' \
    "$(eval_script enqueue-task.lua "$pending" "$active" "$dedupe_two" "$limit" "$topic_key" "$task_ids" , \
        "$queue" task-2 '{"taskId":"task-2","topic":"worker.events","payload":"two"}' \
        1 604800 worker.events)" 1
redis DEL "$dedupe_one" >/dev/null
assert_equals 'duplicate task is ignored' \
    "$(eval_script enqueue-task.lua "$pending" "$active" "$dedupe_one" "$limit" "$topic_key" "$task_ids" , \
        "$queue" task-1 '{"taskId":"task-1","topic":"worker.events","payload":"one"}' \
        1 604800 worker.events)" 0
limit_conflict="$(eval_script enqueue-task.lua "$pending" "$active" "$queue_key:dedupe:dGFzay0z" \
    "$limit" "$topic_key" "$task_ids" , "$queue" task-3 \
    '{"taskId":"task-3","topic":"worker.events","payload":"three"}' 2 604800 worker.events 2>&1 || true)"
assert_contains 'active queue rejects a changed concurrency limit' "$limit_conflict" QUEUE_LIMIT_CONFLICT
topic_conflict="$(eval_script enqueue-task.lua "$pending" "$active" "$queue_key:dedupe:dGFzay00" \
    "$limit" "$topic_key" "$task_ids" , "$queue" task-4 \
    '{"taskId":"task-4","topic":"other.events","payload":"four"}' 1 604800 other.events 2>&1 || true)"
assert_contains 'active queue rejects a changed destination topic' "$topic_conflict" QUEUE_TOPIC_CONFLICT
assert_equals 'configuration conflicts do not enqueue tasks' "$(redis LLEN "$pending")" 2
assert_equals 'in-flight limit promotes one task' \
    "$(eval_script promote-queue.lua "$pending" "$inflight" "$stream" "$active" "$delayed" "$limit" , \
        "$queue" 100 "$(now_ms)")" 1
assert_equals 'full queue does not promote' \
    "$(eval_script promote-queue.lua "$pending" "$inflight" "$stream" "$active" "$delayed" "$limit" , \
        "$queue" 100 "$(now_ms)")" 0
assert_equals 'ACK frees one slot' \
    "$(eval_script acknowledge-task.lua "$pending" "$inflight" "$delayed" "$active" "$limit" "$topic_key" "$task_ids" , \
        "$queue" task-1)" 1
assert_equals 'next FIFO task is promoted' \
    "$(eval_script promote-queue.lua "$pending" "$inflight" "$stream" "$active" "$delayed" "$limit" , \
        "$queue" 100 "$(now_ms)")" 1
assert_equals 'last task ACK drains queue' \
    "$(eval_script acknowledge-task.lua "$pending" "$inflight" "$delayed" "$active" "$limit" "$topic_key" "$task_ids" , \
        "$queue" task-2)" 1
assert_equals 'queue topic metadata is removed after drain' "$(redis GET "$topic_key")" ''

queue='tenant-delayed'
queue_key="$base:q:dGVuYW50LWRlbGF5ZWQ"
pending="$queue_key:pending"
inflight="$queue_key:inflight"
delayed="$queue_key:delayed"
limit="$queue_key:limit"
topic_key="$queue_key:topic"
task_ids="$queue_key:tasks"
dedupe="$queue_key:dedupe:ZGVsYXllZC0x"
due_at="$(( $(now_ms) + 60000 ))"
assert_equals 'schedule delayed task' \
    "$(eval_script schedule-delayed-task.lua "$delayed" "$active" "$dedupe" "$limit" "$topic_key" "$task_ids" , \
        "$queue" delayed-1 '{"taskId":"delayed-1","topic":"worker.events","payload":"later"}' \
        1 "$due_at" 604800 worker.events)" 1
assert_equals 'future task stays delayed' \
    "$(eval_script promote-delayed-tasks.lua "$pending" "$delayed" "$active" , \
        "$queue" "$((due_at - 1))" 100)" 0
assert_equals 'due task moves to FIFO queue' \
    "$(eval_script promote-delayed-tasks.lua "$pending" "$delayed" "$active" , \
        "$queue" "$((due_at + 1))" 100)" 1
assert_equals 'delayed task is promoted' \
    "$(eval_script promote-queue.lua "$pending" "$inflight" "$stream" "$active" "$delayed" "$limit" , \
        "$queue" 100 "$(now_ms)")" 1
assert_equals 'delayed task ACK drains queue' \
    "$(eval_script acknowledge-task.lua "$pending" "$inflight" "$delayed" "$active" "$limit" "$topic_key" "$task_ids" , \
        "$queue" delayed-1)" 1

queue='tenant-fair'
queue_key="$base:q:dGVuYW50LWZhaXI"
pending="$queue_key:pending"
inflight="$queue_key:inflight"
delayed="$queue_key:delayed"
limit="$queue_key:limit"
topic_key="$queue_key:topic"
task_ids="$queue_key:tasks"
dedupe_one="$queue_key:dedupe:Zmlyc3Q"
dedupe_two="$queue_key:dedupe:c2Vjb25k"
assert_equals 'enqueue fairness task one' \
    "$(eval_script enqueue-task.lua "$pending" "$active" "$dedupe_one" "$limit" "$topic_key" "$task_ids" , \
        "$queue" fair-1 '{"taskId":"fair-1","topic":"worker.events","payload":"one"}' \
        5 604800 worker.events)" 1
assert_equals 'enqueue fairness task two' \
    "$(eval_script enqueue-task.lua "$pending" "$active" "$dedupe_two" "$limit" "$topic_key" "$task_ids" , \
        "$queue" fair-2 '{"taskId":"fair-2","topic":"worker.events","payload":"two"}' \
        5 604800 worker.events)" 1
fair_now="$(now_ms)"
assert_equals 'fair queue yields after one promotion' \
    "$(eval_script promote-queue.lua "$pending" "$inflight" "$stream" "$active" "$delayed" "$limit" , \
        "$queue" 1 "$fair_now")" 1
active_score="$(redis ZSCORE "$active" "$queue")"
if (( active_score <= fair_now )); then
    echo "active queue did not yield to other queues" >&2
    exit 1
fi
assert_equals 'ACK reactivates yielded queue' \
    "$(eval_script acknowledge-task.lua "$pending" "$inflight" "$delayed" "$active" "$limit" "$topic_key" "$task_ids" , \
        "$queue" fair-1)" 1
assert_equals 'fair queue resumes after ACK' \
    "$(eval_script promote-queue.lua "$pending" "$inflight" "$stream" "$active" "$delayed" "$limit" , \
        "$queue" 10 "$(now_ms)")" 1
assert_equals 'fair queue drains after ACK' \
    "$(eval_script acknowledge-task.lua "$pending" "$inflight" "$delayed" "$active" "$limit" "$topic_key" "$task_ids" , \
        "$queue" fair-2)" 1

assert_equals 'dispatch group is created' \
    "$(eval_script ensure-dispatch-group.lua "$stream" , scheduler-dispatchers-v1)" 1
assert_equals 'dispatch group creation is idempotent' \
    "$(eval_script ensure-dispatch-group.lua "$stream" , scheduler-dispatchers-v1)" 0
lock="$base:dispatch-lock"
assert_equals 'acquire dispatch lease' "$(redis SET "$lock" lease-token NX PX 30000)" OK
assert_equals 'renew dispatch lease' \
    "$(eval_script renew-dispatch-lock.lua "$lock" , lease-token 30000)" 1
assert_equals 'release dispatch lease' \
    "$(eval_script release-dispatch-lock.lua "$lock" , lease-token)" 1
redis XREADGROUP GROUP scheduler-dispatchers-v1 integration-consumer \
    COUNT 10 STREAMS "$stream" '>' >/dev/null
dispatch_count=0
while [[ "$(redis XLEN "$stream")" != 0 ]]; do
    record_id="$(redis XRANGE "$stream" - + COUNT 1 | head -n 1)"
    assert_equals 'dispatch record is acknowledged' \
        "$(eval_script acknowledge-dispatch.lua "$stream" , scheduler-dispatchers-v1 "$record_id")" 1
    dispatch_count=$((dispatch_count + 1))
done
assert_equals 'all five dispatch records are removed' "$dispatch_count" 5

echo "Redis Lua integration checks passed."
