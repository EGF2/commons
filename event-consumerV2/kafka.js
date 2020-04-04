const Kafka = require('node-rdkafka');
const uuid = require("uuid").v4;
const { argv } = require('yargs');
const Logging = require("../Logging");

const Log = new Logging(__filename);

let currentPartitions = [];

const getOffsetsInfo = consumer => new Promise((resolve, reject) => {
    let assignments = consumer.assignments();
    consumer.committed([...currentPartitions, ...assignments], 3000, (e, data) => {
        if (e) return reject(e);
        const checkAssignments = assignments.map(a => a.partition);
        const checkCurrentPartitions = currentPartitions.map(p => p.partition);
        const result = {
            new: {},
            old: {},
        }
        data.forEach(i => {
            if (checkAssignments.includes(i.partition))
                result.new[`partition ${i.partition}`] = i.offset;

            if (checkCurrentPartitions.includes(i.partition))
                result.old[`partition ${i.partition}`] = i.offset;
        });

        resolve(result);
    })
});

/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */

const getHandler = (config, eventHandler, errorHandler, consumer) => async () => {
    try {
        consumer.subscribe([config.kafka.topic]);
        Log.info("Consumer subscribed on topic", {
            ...consumer.globalConfig,
            offsetStrategy: config.kafka.offsetStrategy || "earliest",
        });

        while (true) {
            // get new message
            const data = await new Promise((resolve, reject) => {
                consumer.consume(1, (err, data) => {
                    if (err) {
                        return reject(err);
                    }
                    resolve(data)
                });
            });
            // if message is not empty, then processing
            if (data.length) {
                // parsing message for input kafkaInfo into event
                const { value, ...message } = data[0];
                const event = { kafkaInfo: message, ...JSON.parse(value.toString()) }

                // service should be down if it gets an old message from Kafka. Default delta = 1 day
                if (new Date() - new Date(message.timestamp) > (config.kafka.maxDeltaTimestamp || 4 * 24 * 60 * 60 * 1000)) {
                    const e = new Error(`Consumer get old message for ${message.timestamp}`);
                    Log.error(`Consumer get old message`, e, { message });
                    throw e;
                }

                // processing
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
                Log.info('Rebalance called. Results', { partitions: (this.assignments()).map(e => e.partition).join() });
                const offsetInfo = await getOffsetsInfo(consumer);
                currentPartitions = this.assignments();

                Log.info("Offsets info", offsetInfo);
            } else if (err.code == Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
                this.unassign();
            } else {
                Log.error("Rebalance error", err, this);
                process.exit(1);
            }
        },
    },
        {
            'auto.offset.reset': config.kafka.offsetStrategy || "earliest",
        });

    // create a handler based on the provided functions
    const handler = getHandler(config, eventHandler, errorHandler, consumer);

    consumer.connect({ timeout: "1000ms" }, (err) => {
        if (err) {
            Log.error(`Error connecting to Kafka broker: `, err, consumer)
            process.exit(1);
        }
        Log.info("Connected to Kafka broker");
    });

    consumer.on('disconnected', (e, data) => {
        Log.error(`Consumer got disconnected: `, e, { ...data, consumer });
        process.exit(1)
    });

    // register ready handler.
    consumer.on('ready', handler);
};

module.exports = newConsumer;
