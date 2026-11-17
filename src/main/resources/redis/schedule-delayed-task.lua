-- Delayed tasks share the queue's fixed in-flight limit and are deduplicated.
local current_limit = redis.call('GET', KEYS[4])
if current_limit and current_limit ~= ARGV[4] then
    return redis.error_reply('QUEUE_LIMIT_CONFLICT')
end

local current_topic = redis.call('GET', KEYS[5])
if current_topic and current_topic ~= ARGV[7] then
    return redis.error_reply('QUEUE_TOPIC_CONFLICT')
end

if redis.call('EXISTS', KEYS[3]) == 1 then
    return 0
end

if redis.call('SISMEMBER', KEYS[6], ARGV[2]) == 1 then
    return 0
end

redis.call('SET', KEYS[4], ARGV[4])
redis.call('SET', KEYS[5], ARGV[7])
redis.call('ZADD', KEYS[1], ARGV[5], ARGV[3])
redis.call('SADD', KEYS[6], ARGV[2])
local active_score = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not active_score or tonumber(active_score) > tonumber(ARGV[5]) then
    redis.call('ZADD', KEYS[2], ARGV[5], ARGV[1])
end
redis.call('SET', KEYS[3], '1', 'EX', ARGV[6])
return 1
