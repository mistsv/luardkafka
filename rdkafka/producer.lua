
local librdkafka = require 'rdkafka.librdkafka'
local KafkaConfig = require 'rdkafka.config'
local KafkaTopic = require 'rdkafka.topic'
local ffi = require 'ffi'

local DEFAULT_DESTROY_TIMEOUT_MS = 3000

local KafkaProducer = {}
KafkaProducer.__index = KafkaProducer


--[[
    Creates a new Kafka producer.

    'kafka_config' is an optional object that will be used instead of the default
    configuration.
    The 'kafka_config' object is reusable after this call.

    'destroy_timeout_ms' is a parameter that is used to determine how long client
    will wait while all rd_kafka_t objects will be destroyed.

    Returns the new object on success or "error(errstr)" on error in which case
    'errstr' is set to a human readable error message.
]]--

function KafkaProducer.create(kafka_config, destroy_timeout_ms)
    local config = nil
    if kafka_config ~= nil then
        config = KafkaConfig.create(kafka_config).kafka_conf_
        ffi.gc(config, nil)
    end

    local ERRLEN = 256
    local errbuf = ffi.new("char[?]", ERRLEN) -- cdata objects are garbage collected
    local kafka = librdkafka.rd_kafka_new(librdkafka.RD_KAFKA_PRODUCER, config, errbuf, ERRLEN)

    if kafka == nil then
        error(ffi.string(errbuf))
    end

    local producer = {kafka_ = kafka}
    KafkaTopic.kafka_topic_map_[kafka] = {}

    setmetatable(producer, KafkaProducer)
    ffi.gc(producer.kafka_, function (...)
        for k, topic_ in pairs(KafkaTopic.kafka_topic_map_[producer.kafka_]) do
            librdkafka.rd_kafka_topic_destroy(topic_) 
        end
        KafkaTopic.kafka_topic_map_[producer.kafka_] = nil
        librdkafka.rd_kafka_destroy(...)
        librdkafka.rd_kafka_wait_destroyed(destroy_timeout_ms or DEFAULT_DESTROY_TIMEOUT_MS)
        end
    )

    return producer
end


--[[
    Adds a one or more brokers to the kafka handle's list of initial brokers.
    Additional brokers will be discovered automatically as soon as rdkafka
    connects to a broker by querying the broker metadata.

    If a broker name resolves to multiple addresses (and possibly
    address families) all will be used for connection attempts in
    round-robin fashion.

    'broker_list' is a ,-separated list of brokers in the format:
    <host1>[:<port1>],<host2>[:<port2>]...

    Returns the number of brokers successfully added.

    NOTE: Brokers may also be defined with the 'metadata.broker.list'
    configuration property.
]]--

function KafkaProducer:brokers_add(broker_list)
    assert(self.kafka_ ~= nil)
    return librdkafka.rd_kafka_brokers_add(self.kafka_, broker_list)
end


--[[
    Produce and send a single message to broker.

    `produce()` is an asynch non-blocking API.

    'partition' is the target partition, either:
     - RD_KAFKA_PARTITION_UA (unassigned) for
        automatic partitioning using the topic's partitioner function, or
     - a fixed partition (0..N)

    'payload' is the message payload.

    'key' is an optional message key, if non-nil it will be passed to the topic
    partitioner as well as be sent with the message to the broker and passed
    on to the consumer.


    Returns "error(errstr)" on error in which case 'errstr' is set to a human
    readable error message.
]]--

function KafkaProducer:produce(kafka_topic, partition, payload, key)
    assert(self.kafka_ ~= nil)
    assert(kafka_topic.topic_ ~= nil)

    local keylen = 0
    if key then keylen = #key end

    if payload == nil or #payload == 0 then
        if keylen == 0 then
            return
        end
    end

    local RD_KAFKA_MSG_F_COPY = 0x2
    local produce_result = librdkafka.rd_kafka_produce(kafka_topic.topic_, partition, RD_KAFKA_MSG_F_COPY,
        ffi.cast("void*", payload), #payload, ffi.cast("void*", key), keylen, nil) 

    if produce_result == -1 then
        error(ffi.string(librdkafka.rd_kafka_err2str(librdkafka.rd_kafka_errno2err(ffi.errno()))))
    end
end

--[[
    Polls the provided kafka handle for events.

    Events will cause application provided callbacks to be called.

    The 'timeout_ms' argument specifies the minimum amount of time
    (in milliseconds) that the call will block waiting for events.
    For non-blocking calls, provide 0 as 'timeout_ms'.
    To wait indefinately for an event, provide -1.

    Events:
    - delivery report callbacks  (if dr_cb is configured) [producer]
    - error callbacks (if error_cb is configured) [producer & consumer]
    - stats callbacks (if stats_cb is configured) [producer & consumer]

    Returns the number of events served.

    NOTE: This function doesn't use jit compilation
]]--

function KafkaProducer:poll(timeout_ms)
    assert(self.kafka_ ~= nil)
    return librdkafka.rd_kafka_poll(self.kafka_, timeout_ms)
end
jit.off(KafkaProducer.poll)

--[[
    Returns the current out queue length:
    messages waiting to be sent to, or acknowledged by, the broker.
]]--

function KafkaProducer:outq_len()
    assert(self.kafka_ ~= nil)
    return librdkafka.rd_kafka_outq_len(self.kafka_)
end

--[[
    Retrieve the current number of threads in use by librdkafka.
]]--

function KafkaProducer.thread_cnt()
    return librdkafka.rd_kafka_thread_cnt()
end

return KafkaProducer
