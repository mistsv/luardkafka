
local librdkafka = require 'rdkafka.librdkafka'
local ffi = require 'ffi'

local KafkaTopicConfig = {}
KafkaTopicConfig.__index = KafkaTopicConfig

--[[
    Create topic configuration object.
]]--

function KafkaTopicConfig.create(original_config)
    local config = {}
    setmetatable(config, KafkaTopicConfig)

    if original_config and original_config.topic_conf_ then
        rawset(config, "topic_conf_", librdkafka.rd_kafka_topic_conf_dup(original_config.topic_conf_))
    else
        rawset(config, "topic_conf_", librdkafka.rd_kafka_topic_conf_new())
    end
    ffi.gc(config.topic_conf_, function (config)
        librdkafka.rd_kafka_topic_conf_destroy(config)
    end
    )
    
    return config
end


--[[
    Dump the topic configuration properties and values of `conf` to a map
    with "key", "value" pairs. 
]]--

function KafkaTopicConfig:dump()
    assert(self.topic_conf_ ~= nil)

    local size = ffi.new("size_t[1]")
    local dump = librdkafka.rd_kafka_topic_conf_dump(self.topic_conf_, size)
    ffi.gc(dump, function(d) librdkafka.rd_kafka_conf_dump_free(d, size[0]) end)

    local result = {}
    for i = 0, tonumber(size[0])-1,2 do
        result[ffi.string(dump[i])] = ffi.string(dump[i+1])
    end

    return result
end


--[[
    Sets a configuration property.

    In case of failure "error(errstr)" is called and 'errstr'
    is updated to contain a human readable error string.
]]--

function KafkaTopicConfig:__newindex(name, value)
    assert(self.topic_conf_ ~= nil)

    local ERRLEN = 256
    local errbuf = ffi.new("char[?]", ERRLEN) -- cdata objects are garbage collected

    if librdkafka.rd_kafka_topic_conf_set(self.topic_conf_, name, value, errbuf, ERRLEN) ~= librdkafka.RD_KAFKA_CONF_OK then
        error(ffi.string(errbuf))
    end
end

return KafkaTopicConfig
