-- Completion is idempotent. A freed slot immediately wakes the queue if work remains.
if redis.call('LLEN', KEYS[1]) > 0 then
    redis.call('ZADD', KEYS[4], 0, ARGV[1])
    local removed = redis.call('HDEL', KEYS[2], ARGV[2])
    if removed == 1 then
        redis.call('SREM', KEYS[7], ARGV[2])
    end
    return removed
end

local next_due = redis.call('ZRANGE', KEYS[3], 0, 0, 'WITHSCORES')
if #next_due > 0 then
    redis.call('ZADD', KEYS[4], next_due[2], ARGV[1])
else
    redis.call('ZREM', KEYS[4], ARGV[1])
end

local removed = redis.call('HDEL', KEYS[2], ARGV[2])
if removed == 1 then
    redis.call('SREM', KEYS[7], ARGV[2])
end
if redis.call('HLEN', KEYS[2]) == 0 and redis.call('LLEN', KEYS[1]) == 0
        and redis.call('ZCARD', KEYS[3]) == 0 and redis.call('SCARD', KEYS[7]) == 0 then
    redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[5], KEYS[6], KEYS[7])
end

return removed
