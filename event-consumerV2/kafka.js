const Kafka = require('node-rdkafka');
const uuid = require("uuid").v4;
const readline = require('readline');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

const QUESTION = `You are trying to manually subscribe to partition ${process.env.debugP}.
If you do this, the service on Amazon will not stop reading this partition and this can lead to unexpected consequences. 
Are you sure you want to continue? (Y/N)`

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
                await eventHandler(JSON.parse(message.value.toString()));
                consumer.commitMessage(message);
            }
        }
    } catch (err) {
        errorHandler(err);
    }
};

const newConsumer = async (config, eventHandler, errorHandler) => {

    // warning message for debug user
    if (process.env.debugP) await new Promise((resolve, reject) => {
        rl.question(QUESTION, answer => {
            if (["N", "No"].includes(answer)) delete process.env.debugP;
            else if (isNaN(Number(process.env.debugP))) reject(new Error(`Invalid partition value ${process.env.debugP}. Partition must be number`))
            resolve()
        });
    });

    const consumer = new Kafka.KafkaConsumer({
        'group.id': config.kafka.groupId,
        'metadata.broker.list': config.kafka.hosts[0],
        'enable.auto.offset.store': false,
        'client.id': `${config.kafka.groupId}${uuid()}`,
        'rebalance_cb': function (err, assignment) {
            let result = [];
            if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
                // assign to debug partition if it is specified
                result = process.env.debugP
                    ? [{ topic: config.kafka.topic, partition: Number(process.env.debugP)}]
                    : [...assignment]
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
