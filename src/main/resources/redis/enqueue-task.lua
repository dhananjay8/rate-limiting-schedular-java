-- Atomically appends one task and registers its queue. The first enqueue fixes
-- the queue's in-flight limit until that queue drains.
local current_limit = redis.call('GET', KEYS[4])
if current_limit and current_limit ~= ARGV[4] then
    return redis.error_reply('QUEUE_LIMIT_CONFLICT')
end

local current_topic = redis.call('GET', KEYS[5])
if current_topic and current_topic ~= ARGV[6] then
    return redis.error_reply('QUEUE_TOPIC_CONFLICT')
end

if redis.call('EXISTS', KEYS[3]) == 1 then
    return 0
end

if redis.call('SISMEMBER', KEYS[6], ARGV[2]) == 1 then
    return 0
end

local queue_was_empty = redis.call('LLEN', KEYS[1]) == 0
redis.call('SET', KEYS[4], ARGV[4])
redis.call('SET', KEYS[5], ARGV[6])
redis.call('RPUSH', KEYS[1], ARGV[3])
redis.call('SADD', KEYS[6], ARGV[2])
if queue_was_empty or not redis.call('ZSCORE', KEYS[2], ARGV[1]) then
    redis.call('ZADD', KEYS[2], 0, ARGV[1])
end
redis.call('SET', KEYS[3], '1', 'EX', ARGV[5])
return 1
