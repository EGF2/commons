const kafka = require("no-kafka");

/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */

const handler = (eventHandler, errorHandler, consumer) => async (messageSet, topic, partition) => {
    try {
        for (const message of messageSet) {
            await eventHandler(JSON.parse(message.message.value.toString("utf8")))
            await consumer.commitOffset({
                topic,
                partition,
                offset: message.offset
            });
        }
    } catch (e) {
        errorHandler(e, consumer);
    }
};

const newConsumer = async (config, eventHandler, errorHandler) => {
    const consumer = new kafka.SimpleConsumer({
        connectionString: config.kafka.hosts.join(","),
        groupId: `${config["consumer-group"]}V2`,
        clientId: config.kafka["client-id"]
    });

    await consumer.init();
    const fetchOffset = await consumer.fetchOffset([{
        topic: config.kafka.topicV2,
        partition: 0
    }]);
    let offset = fetchOffset[0].offset;
    if (offset > -1) {
        offset++;
    }
    consumer.subscribe(config.kafka.topic, 0, { offset }, handler(eventHandler, errorHandler, consumer));
};

module.exports = newConsumer;
