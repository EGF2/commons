const kafka = require("kafka-node");
const Transform = require("stream").Transform;
/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */

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
        console.log("Error kafka", e);
        errorHandler(e);
    });

    consumerGroup.on("connect", () => {
        console.log("Kafka connect", JSON.stringify({host: options.kafkaHost, groupId: options.groupId, topic}));
    });

    consumerGroup.pipe(messageTransform).pipe(resultProducer);
};

module.exports = newConsumer;
