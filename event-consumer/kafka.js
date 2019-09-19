const {Kafka} = require("kafkajs");

/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */

const newConsumer = async (config, eventHandler, errorHandler, options = {}) => {
    const kafka = new Kafka({
        clientId: config.kafka["client-id"],
        brokers: config.kafka.hosts,
        retry: {
            initialRetryTime: 1000,
            retries: 20,
            multiplier: 1.2
        }
    });

    const {groupId: optGroupId} = options;
    const groupId = optGroupId || config["consumer-group"];

    const consumer = kafka.consumer({groupId});
    await consumer.connect();
    await consumer.subscribe({topic: config.kafka.topic, fromBeginning: false});

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
