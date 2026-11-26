-- Adjust a bucket's counters by signed deltas, clamping each field at zero.
-- KEYS[1] is the bucket hash.
-- ARGV[1] is delta_messages, ARGV[2] is delta_tokens, ARGV[3] is the key TTL in seconds.
-- Returns {messages, tokens}.
local key = KEYS[1]
local dmsg = tonumber(ARGV[1])
local dtok = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

if dmsg <= 0 and dtok <= 0 and redis.call('EXISTS', key) == 0 then
    return {0, 0}
end

local msgs = tonumber(redis.call('HGET', key, 'messages')) or 0
local toks = tonumber(redis.call('HGET', key, 'tokens')) or 0

if dmsg ~= 0 then
    msgs = msgs + dmsg
    if msgs < 0 then
        msgs = 0
    end
    redis.call('HSET', key, 'messages', msgs)
end
if dtok ~= 0 then
    toks = toks + dtok
    if toks < 0 then
        toks = 0
    end
    redis.call('HSET', key, 'tokens', toks)
end
if dmsg ~= 0 or dtok ~= 0 then
    redis.call('EXPIRE', key, ttl)
end
return {msgs, toks}
