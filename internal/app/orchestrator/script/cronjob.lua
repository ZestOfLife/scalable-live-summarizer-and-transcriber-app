-- KEYS[1]: audio_key
-- KEYS[2]: visual_key
-- ARGV[1]: min_audio_count
-- ARGV[2]: min_visual_count

-- Size count
local a_count = redis.call('ZCARD', KEYS[1])
local v_count = redis.call('ZCARD', KEYS[2])

-- Threshold logic to trigger summarization
if a_count >= tonumber(ARGV[1]) and v_count >= tonumber(ARGV[2]) then
    local audio_data = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1)
    local visual_data = redis.call('ZRANGE', KEYS[2], 0, tonumber(ARGV[2]) - 1)
    
    redis.call('ZREMRANGEBYRANK', KEYS[1], 0, tonumber(ARGV[1]) - 1)
    redis.call('ZREMRANGEBYRANK', KEYS[2], 0, tonumber(ARGV[2]) - 1)
    
    return {audio_data, visual_data}
else
    return {}
end