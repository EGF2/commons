const kafka = require("kafka-node");
const Transform = require("stream").Transform;
const Logging = require("../Logging");

/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */

const Log = new Logging(__filename);

const newConsumer = async (config, eventHandler, errorHandler) => {
    const topic = config.kafka.topicV2;
    const options = {
        kafkaHost: config.kafka.hosts.join(","),
        groupId: config["consumer-groupV2"],
        protocol: ["roundrobin"],
        encoding: "utf8",
        onRebalance: (isAlreadyMember, callback) => {
            callback();
        },
        fromOffset: "latest",
        outOfRangeOffset: "earliest",
        autoCommit: false
    };
    const consumerGroup = new kafka.ConsumerGroupStream(
    options,
    topic
  );

    const messageTransform = new Transform({
        objectMode: true,
        decodeStrings: true,
        async transform(message, encoding, callback) {
            try {
                await eventHandler(JSON.parce(message.value));
            } catch (e) {
                errorHandler(e);
            }
            consumerGroup.commit(message, true, () => {});
            callback(null, {
                topic,
                messages: message
            });
        }
    });

    consumerGroup.on("error", e => {
        Log.error("Error kafka", e);
        errorHandler(e);
    });

    consumerGroup.on("connect", () => {
        Log.info("Kafka connect", {host: options.kafkaHost, groupId: options.groupId, topic});
    });

    consumerGroup.pipe(messageTransform);
};

module.exports = newConsumer;
