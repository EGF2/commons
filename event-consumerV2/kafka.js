const Kafka = require('node-rdkafka');
const uuid = require("uuid").v4;

/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */

const getHandler = (eventHandler, errorHandler, consumer) => async () => {
    try {
        consumer.subscribe([config.topic]);
        console.log(`Consumer ${consumer.name} subscribed on ${config.topic}`)
        // noinspection InfiniteLoopJS
        while (true) {
            const data = await new Promise((resolve, reject) => {
                consumer.consume(1, (err, data) => {
                    if (err) {
                        return reject(err);
                    }
                    resolve(data)
                });
            });
            if (data.length > 0) {
                // new messages will start arriving after re-balance and new
                // assignments are given to us.
                await eventHandler(data[0]);
            }
        }
    } catch (err) {
        errorHandler(err);
    }
};

const onRebalance = (err, assignment) => {
    if (err) {
        console.log("Rebalance error: ", err);
        process.exit(1);
    }
    console.log('Rebalance called. Results', assignment.map(el => el.partition).join());
}

const newConsumer = async (config, eventHandler, errorHandler) => {
    const consumer = new Kafka.KafkaConsumer({
        'group.id': config.kafka.group,
        'metadata.broker.list': config.kafka.host,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': false,
        'enable.auto.offset.store': false,
        'client.id': `${config.kafka.group}${uuid()}`,
        'rebalance_cb': onRebalance,
    });
    const handler = getHandler(eventHandler, errorHandler, consumer);

    // Connect the consumer.
    consumer.connect({ timeout: "1000ms" }, (err) => {
        if (err) {
            console.log(`Error connecting to Kafka broker: `, err);
            process.exit(1);
        }
        console.log("Connected to Kafka broker");
    });

    consumer.on('disconnected', (args) => {
        console.error(`Consumer got disconnected: ${JSON.stringify(args)}`);
        process.exit(1)
    });

    // register ready handler.
    consumer.on('ready', handler);
};

module.exports = newConsumer;
