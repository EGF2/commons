const {Kafka} = require("kafkajs");

/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */

const newConsumer = async (config, eventHandler, errorHandler) => {
    const kafka = new Kafka({
        clientId: config.kafka["client-id"],
        brokers: config.kafka.hosts
    });

    const consumer = kafka.consumer({groupId: `${config["consumer-group"]}V2`});

    await consumer.connect();
    await consumer.subscribe({topic: config.kafka.topicV2, fromBeginning: false});

    await consumer.run({
        eachBatchAutoResolve: false,
        eachBatch: async ({batch, resolveOffset, heartbeat, isRunning, isStale}) => {
            for (const message of batch.messages) {
                if (!isRunning() || isStale()) {
                    break;
                }
                try {
                    await eventHandler(JSON.parse(message.value.toString("utf8")));
                    await resolveOffset(message.offset);
                    await heartbeat();
                } catch (e) {
                    errorHandler(e, consumer);
                }
            }
        }
    });
};

module.exports = newConsumer;
