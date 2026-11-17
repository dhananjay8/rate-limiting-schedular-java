-- Remove a dispatch record only after Kafka confirms the send.
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if acknowledged == 1 then
    redis.call('XDEL', KEYS[1], ARGV[2])
end
return acknowledged
