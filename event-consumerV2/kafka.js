const kafka = require("kafka-node");
const Transform = require("stream").Transform;
/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */

const newConsumer = async (config, eventHandler, errorHandler, Logger) => {
    const Log = new Logger(__filename);
    const topic = config.kafka.topicV2;
    const options = {
        kafkaHost: config.kafka.hosts.join(","),
        groupId: config.groupId,
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
    const resultProducer = new kafka.ProducerStream();
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

    consumerGroup.pipe(messageTransform).pipe(resultProducer);
};

module.exports = newConsumer;
