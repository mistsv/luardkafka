
local BROKERS_ADDRESS = { "localhost" }
local TOPIC_NAME = "test_topic"


local config = require 'rdkafka.config'.create()

config["statistics.interval.ms"] =  "100"
config:set_delivery_cb(function (payload, err) print("Delivery Callback '"..payload.."'") end)
config:set_stat_cb(function (payload) print("Stat Callback '"..payload.."'") end)

local producer = require 'rdkafka.producer'.create(config)

for k, v in pairs(BROKERS_ADDRESS) do
    producer:brokers_add(v)
end

local topic_config = require 'rdkafka.topic_config'.create()
topic_config["auto.commit.enable"] = "true"

local topic = require 'rdkafka.topic'.create(producer, TOPIC_NAME, topic_config)

local KAFKA_PARTITION_UA = -1

for i = 0,10 do
    producer:produce(topic, KAFKA_PARTITION_UA, "this is test message"..tostring(i))
end

while producer:outq_len() ~= 0 do
    producer:poll(10)
end
