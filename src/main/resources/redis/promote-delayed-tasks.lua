-- Moves only due entries into the FIFO queue and updates its next wake-up score.
local due_tasks = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[2],
        'LIMIT', 0, ARGV[3])
local moved = 0

for _, task_json in ipairs(due_tasks) do
    if redis.call('RPUSH', KEYS[1], task_json) then
        if redis.call('ZREM', KEYS[2], task_json) == 1 then
            moved = moved + 1
        else
            redis.call('LREM', KEYS[1], 1, task_json)
        end
    end
end

if redis.call('LLEN', KEYS[1]) > 0 then
    if not redis.call('ZSCORE', KEYS[3], ARGV[1]) then
        redis.call('ZADD', KEYS[3], 0, ARGV[1])
    end
else
    local next_due = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
    if #next_due > 0 then
        redis.call('ZADD', KEYS[3], next_due[2], ARGV[1])
    else
        redis.call('ZREM', KEYS[3], ARGV[1])
    end
end

return moved
