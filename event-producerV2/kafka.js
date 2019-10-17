const kafka = require("kafka-node");
const Logging = require("../Logging");

const Producer = kafka.Producer;
const Log = new Logging(__filename);

module.exports = async (event, config, distribution) => {
    const client = new kafka.KafkaClient(config.kafka.hosts.join(","), config.kafka["client-id"]);

    const {partition, distributionFn} = distribution;
    const partitionerType = distributionFn ? 4 : 2;

    const prodArgs = [client, {partitionerType}];
    if (distributionFn) {
        prodArgs.push(distributionFn);
    }

    const producer = new Producer(...prodArgs);

    const payloads = [
        {
            topic: config.kafka.topicV2,
            messages: JSON.stringify(event)
        }
    ];
    if (partition) {
        payloads[0].partition = partition;
    }

    const getReady = () => {
        return new Promise(resolve => {
            producer.on("ready", function() {
                resolve();
            });
        });
    };

    producer.on("error", e => {
        Log.error("Kafka producer error.", e);
    });

    const result = await getReady().then(() => {
        return new Promise((resolve, reject) => {
            producer.send(payloads, (err, data) => {
                if (err) {
                    Log.error("Kafka producer error.", err);
                    reject(err);
                }
                resolve(data);
            });
        });
    });

    return result;
};
