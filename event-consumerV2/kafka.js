const Kafka = require('node-rdkafka');
const uuid = require("uuid").v4;
const { argv } = require('yargs');

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
                const { value, ...message } = data[0];
                const event = { kafkaInfo: message, ...JSON.parse(value.toString())}
                await eventHandler(event);
                consumer.commitMessage(message);
            }
        }
    } catch (err) {
        errorHandler(err);
    }
};

const newConsumer = async (config, eventHandler, errorHandler) => {

    // for debug
    let debugPartitions = null;
    if (argv.p && argv.p !== 0)
        debugPartitions = `${argv.p}`
            .split(',')
            .map(p => {
                // check debug partition
                if (isNaN(Number(p)) || p === "")
                    throw new Error(`Invalid partition value ${argv.p}. Partition must be number or list of numbers (example: 1 or 1,2,3).`);

                // mapping debug partition
                return { topic: config.kafka.topic, partition: Number(p) }
            })

    const consumer = new Kafka.KafkaConsumer({
        'group.id': config.kafka.groupId,
        'metadata.broker.list': config.kafka.hosts[0],
        'enable.auto.offset.store': false,
        'client.id': `${config.kafka.groupId}${uuid()}`,
        'rebalance_cb': async function (err, assignment) {
            if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
                let result = [];

                // select debug partition if it is specified
                if (debugPartitions) {
                    console.log(
                        "\x1b[31m",
                        `You are trying to manually subscribe to partitions ${process.env.debugP}. If you do this, the service on Amazon will not stop reading this partition and this can lead to unexpected consequences.`
                            .toUpperCase(),
                        "\x1b[0m"
                    );
                    result = debugPartitions;
                } else result = [...assignment];

                // assign to partitions
                this.assign(result);
                console.log('Rebalance called. Results', result.map(e => e.partition).join());
            } else if (err.code == Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
                this.unassign();
            } else {
                console.error(err);
            }
        },
    },
        {
            'auto.offset.reset': config.kafka.offsetStrategy || "earliest",
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
