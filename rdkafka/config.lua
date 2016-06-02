
local librdkafka = require 'rdkafka.librdkafka'
local ffi = require 'ffi'

local KafkaConfig = {}
KafkaConfig.__index = KafkaConfig

--[[
    Create configuration object or dublicate one.
    Result will be set up the defaults.
    
    Please see CONFIGURATION.md for the default settings. 
]]--

function KafkaConfig.create(original_config)
    local config = { cb_ = {} }
    setmetatable(config, KafkaConfig)

    if original_config and original_config.kafka_conf_ then
        rawset(config, "kafka_conf_", librdkafka.rd_kafka_conf_dup(original_config.kafka_conf_))
        config:set_delivery_cb(original_config.cb_.dr_cb_)
        config:set_stat_cb(original_config.cb_.stat_cb_)
        config:set_error_cb(original_config.cb_.error_cb_)
        config:set_log_cb(original_config.cb_.log_cb_)
    else
        rawset(config, "kafka_conf_", librdkafka.rd_kafka_conf_new())
    end
    ffi.gc(config.kafka_conf_, function (config)
        librdkafka.rd_kafka_conf_destroy(config)
    end
    )
    
    return config
end


--[[
    Dump the configuration properties and values of `conf` to a map
    with "key", "value" pairs. 
]]--

function KafkaConfig:dump()
    assert(self.kafka_conf_ ~= nil)

    local size = ffi.new("size_t[1]")
    local dump = librdkafka.rd_kafka_conf_dump(self.kafka_conf_, size)
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

function KafkaConfig:__newindex(name, value)
    assert(self.kafka_conf_ ~= nil)

    local ERRLEN = 256
    local errbuf = ffi.new("char[?]", ERRLEN) -- cdata objects are garbage collected

    if librdkafka.rd_kafka_conf_set(self.kafka_conf_, name, tostring(value), errbuf, ERRLEN) ~= librdkafka.RD_KAFKA_CONF_OK then
        error(ffi.string(errbuf))
    end
end


--[[
    Set delivery report callback in provided conf object.

    Format: callback_function(payload, errstr)
    'payload' is the message payload
    'errstr' nil if everything is ok or readable error description otherwise
]]--

function KafkaConfig:set_delivery_cb(callback)
    assert(self.kafka_conf_ ~= nil)

    if callback then
        self.cb_.dr_cb_ = callback
        librdkafka.rd_kafka_conf_set_dr_cb(self.kafka_conf_,
            function(rk, payload, len, err)
                local errstr = nil
                if err ~= librdkafka.RD_KAFKA_RESP_ERR_NO_ERROR then
                    errstr = ffi.string(librdkafka.rd_kafka_err2str(err))
                end
                callback(ffi.string(payload, tonumber(len)), errstr)
            end)
    end
end


--[[
    Set statistics callback.
    The statistics callback is called from `KafkaProducer:poll` every
    `statistics.interval.ms` (needs to be configured separately).

    Format: callback_function(json)
    'json' - String containing the statistics data in JSON format
]]--

function KafkaConfig:set_stat_cb(callback)
    assert(self.kafka_conf_ ~= nil)

    if callback then
        self.cb_.stat_cb_ = callback
        librdkafka.rd_kafka_conf_set_stats_cb(self.kafka_conf_,
            function(rk, json, json_len)
                callback(ffi.string(json, json_len))
                return 0 --librdkafka will immediately free the 'json' pointer.
            end)
    end
end


--[[
    Set error callback.
    The error callback is used by librdkafka to signal critical errors
    back to the application.

    Format: callback_function(err_numb, reason)
]]--

function KafkaConfig:set_error_cb(callback)
    assert(self.kafka_conf_ ~= nil)

    if callback then
        self.cb_.error_cb_ = callback
        librdkafka.rd_kafka_conf_set_error_cb(self.kafka_conf_,
            function(rk, err, reason)
                callback(tonumber(err), ffi.string(reason))
            end)
    end
end

--[[
    Set logger callback.
    The default is to print to stderr.
    Alternatively the application may provide its own logger callback.
    Or pass 'callback' as nil to disable logging.

    Format: callback_function(level, fac, buf)
]]--

function KafkaConfig:set_log_cb(callback)
    assert(self.kafka_conf_ ~= nil)

    if callback then
        self.cb_.log_cb_ = callback
        librdkafka.rd_kafka_conf_set_log_cb(self.kafka_conf_,
            function(rk, level, fac, buf)
                callback(tonumber(level), ffi.string(fac), ffi.string(buf))
            end)
    end
end

return KafkaConfig
