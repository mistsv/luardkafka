
local librdkafka = require 'rdkafka.librdkafka'
local KafkaTopicConfig = require 'rdkafka.topic_config'
local ffi = require 'ffi'

local KafkaTopic = { kafka_topic_map_ = {} }
-- KafkaProducer will delete all topics on destroy
-- It was done in order to avoid destroing topics before destroing producer

KafkaTopic.__index = KafkaTopic

--[[
    Creates a new topic handle for topic named 'topic_name'.

    'conf' is an optional configuration for the topic  that will be used
    instead of the default topic configuration.
    The 'conf' object is reusable after this call.

    Returns the new topic handle or "error(errstr)" on error in which case
    'errstr' is set to a human readable error message.
]]--

function KafkaTopic.create(kafka_producer, topic_name, topic_config)
    assert(kafka_producer.kafka_ ~= nil)

    local config = nil
    if topic_config and topic_config.topic_config_ then
        config = KafkaTopicConfig.create(topic_config).topic_conf_
        ffi.gc(config, nil)
    end

    local rd_topic = librdkafka.rd_kafka_topic_new(kafka_producer.kafka_, topic_name, config)
    
    if rd_topic == nil then
        error(ffi.string(librdkafka.rd_kafka_err2str(librdkafka.rd_kafka_errno2err(ffi.errno()))))
    end

    local topic = {topic_ = rd_topic}
    setmetatable(topic, KafkaTopic)
    table.insert(KafkaTopic.kafka_topic_map_[kafka_producer.kafka_], rd_topic)
    return topic
end


--[[
    Returns the topic name
]]--

function KafkaTopic:name()
    assert(self.topic_ ~= nil)
    return ffi.string(librdkafka.rd_kafka_topic_name(self.topic_))
end

return KafkaTopic
