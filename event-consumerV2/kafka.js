const Kafka = require('node-rdkafka');
const uuid = require("uuid").v4;

/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */

const getHandler = (config, eventHandler, errorHandler, consumer) => async () => {
    try {
        consumer.subscribe([config.kafka.topic]);
        console.log(`Consumer ${consumer.name} subscribed on ${config.kafka.topic}`)

        while (true) {
            const data = await new Promise((resolve, reject) => {
                consumer.consume(1, (err, data) => {
                    if (err) {
                        return reject(err);
                    }
                    resolve(data)
                });
            });
            if (data.length) {
                const message = data[0];
                console.log("kafka info", message.offset);

                await eventHandler(JSON.parse(message.value.toString()));
                consumer.commitMessage(message);
            }
        }
    } catch (err) {
        errorHandler(err);
    }
};

const newConsumer = async (config, eventHandler, errorHandler) => {
    const consumer = new Kafka.KafkaConsumer({
        'group.id': config.kafka.groupId,
        'metadata.broker.list': config.kafka.hosts[0],
        'enable.auto.offset.store': false,
        'client.id': `${config.kafka.groupId}${uuid()}`,
        'rebalance_cb': function (err, assignment) {
            console.log('Rebalance called. Results', assignment.map(e => e.partition).join());
            if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
                this.assign(assignment);
            } else if (err.code == Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
                this.unassign();
            } else {
                console.error(err);
            }
        },
    },
        {
            'auto.offset.reset': 'earliest',
        });
    const handler = getHandler(config, eventHandler, errorHandler, consumer);

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
