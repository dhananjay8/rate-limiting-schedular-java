-- Reserves capacity and appends dispatch records atomically within one shard.
local capacity = tonumber(redis.call('GET', KEYS[6]) or '0')
if capacity < 1 then
    return redis.error_reply('QUEUE_LIMIT_MISSING')
end

local available = capacity - redis.call('HLEN', KEYS[2])
local batch_limit = tonumber(ARGV[2])
local moved = 0

while available > 0 and moved < batch_limit do
    local task_json = redis.call('LINDEX', KEYS[1], 0)
    if not task_json then
        break
    end

    local task = cjson.decode(task_json)
    local stream_id = redis.call('XADD', KEYS[3], '*',
            'queueId', ARGV[1],
            'taskId', task.taskId,
            'topic', task.topic,
            'payload', task.payload)
    local reservation = redis.pcall('HSET', KEYS[2], task.taskId, task_json)
    if type(reservation) == 'table' and reservation.err then
        redis.call('XDEL', KEYS[3], stream_id)
        return redis.error_reply(reservation.err)
    end
    redis.call('LPOP', KEYS[1])
    moved = moved + 1
    available = available - 1
end

if redis.call('LLEN', KEYS[1]) > 0 and redis.call('HLEN', KEYS[2]) < capacity then
    redis.call('ZADD', KEYS[4], tonumber(ARGV[3]) + 1, ARGV[1])
else
    local next_due = redis.call('ZRANGE', KEYS[5], 0, 0, 'WITHSCORES')
    if #next_due > 0 then
        redis.call('ZADD', KEYS[4], next_due[2], ARGV[1])
    else
        redis.call('ZREM', KEYS[4], ARGV[1])
    end
end

return moved
