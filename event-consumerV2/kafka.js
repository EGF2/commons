const kafka = require("kafka-node");
const Transform = require("stream").Transform;
const Logging = require("../Logging");
const StatusEmitter = require("./status_emitter");

/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */

const Log = new Logging(__filename);

const newConsumer = async (config, eventHandler, errorHandler) => {
    const emitter = new StatusEmitter(config);

    const errorWrapper = (e, errorHandler) => {
        emitter.sendStatus("DOWN");
        errorHandler(e);
    };

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
        autoCommit: false,
        connectTimeout: 10000,
        requestTimeout: 30000,
        idleConnection: 5 * 60 * 1000,
        reconnectOnIdle: true,
        autoConnect: true,
        connectRetryOptions: {
            forever: true,
            factor: 1,
            minTimeout: 2 * 1000,
            maxTimeout: 4 * 1000,
            randomize: false,
        },
    };
    const consumerGroup = new kafka.ConsumerGroupStream(options, topic);

    const messageTransform = new Transform({
        objectMode: true,
        decodeStrings: true,
        async transform(message, encoding, callback) {
            try {
                await eventHandler(JSON.parse(message.value));
            } catch (e) {
                errorWrapper(e, errorHandler);
            }
            consumerGroup.commit(message, true, () => {});
            callback(null, {
                topic,
                messages: message,
            });
        },
    });

    consumerGroup.on("error", e => {
        Log.error("Error kafka", e);
        errorWrapper(e, errorHandler);
    });

    consumerGroup.on("connect", () => {
        Log.info("Kafka connect", { host: options.kafkaHost, groupId: options.groupId, topic });
        emitter.sendStatus("UP");
    });

    consumerGroup.pipe(messageTransform);
};

module.exports = newConsumer;
