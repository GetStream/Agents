-- Atomically check a bucket's daily limits and reserve one message plus an optional token hold.
-- KEYS[1] is the bucket hash.
-- ARGV[1] is messages_per_day (0 = unenforced), ARGV[2] is tokens_per_day (0 = unenforced),
-- ARGV[3] is the token hold to reserve (>= 0), ARGV[4] is the key TTL in seconds.
-- Returns {admitted (1 or 0), messages, tokens}.
local key = KEYS[1]
local msg_limit = tonumber(ARGV[1])
local tok_limit = tonumber(ARGV[2])
local tok_hold = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local msgs = tonumber(redis.call('HGET', key, 'messages')) or 0
local toks = tonumber(redis.call('HGET', key, 'tokens')) or 0

if (msg_limit > 0 and msgs >= msg_limit) or (tok_limit > 0 and toks >= tok_limit) then
    return {0, msgs, toks}
end

msgs = redis.call('HINCRBY', key, 'messages', 1)
if tok_hold > 0 then
    toks = redis.call('HINCRBY', key, 'tokens', tok_hold)
end
redis.call('EXPIRE', key, ttl)
return {1, msgs, toks}
