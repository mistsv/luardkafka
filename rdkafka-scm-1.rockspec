package = 'rdkafka'
version = 'scm-1'
source  = {
    url    = 'git://github.com/mistsv/luardkafka.git',
    branch = 'master',
}
description = {
    summary  = "Lua binding for librdkafka",
    homepage = 'https://github.com/mistsv/luardkafka.git',
    license  = 'MIT',
}
dependencies = {
    'lua >= 5.1'
}
build = {
    type = 'builtin',

    modules = {
        ['rdkafka.config']            = 'rdkafka/config.lua',
        ['rdkafka.librdkafka']        = 'rdkafka/librdkafka.lua',
        ['rdkafka.producer']          = 'rdkafka/producer.lua',
        ['rdkafka.topic_config']      = 'rdkafka/topic_config.lua',
        ['rdkafka.topic']             = 'rdkafka/topic.lua'
    }
}

-- vim: syntax=lua