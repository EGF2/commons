const EventEmitter = require("events");
const axios = require("axios");
const emitter = new EventEmitter();

/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */

const handler = (config, eventHandler, errorHandler) => {
    return async () => {
        try {
            // get message list
            const messages = (await axios({
                method: "GET",
                url: `http://${config.kafka.api}/consumers/${config.kafka.group_id}/instances/${config.kafka.me.instance_id}/records`,
                headers: {
                    "Content-Type": "application/vnd.kafka.v2+json",
                    Host: config.kafka.api
                }
            })).data;
            for (const message of messages) {
                // parsing event
                const event = JSON.parse(Buffer.from(message.value, "base64").toString("ascii"));

                // processing
                await eventHandler(event);

                // commit message
                await axios({
                    method: "POST",
                    url: `http://${config.kafka.api}/consumers/${config.kafka.group_id}/instances/${config.kafka.me.instance_id}/offsets`,
                    headers: {
                        "Content-Type": "application/vnd.kafka.v2+json",
                        Host: config.kafka.api
                    },
                    data: {
                        offsets: [{
                            offset: message.offset,
                            partition: message.partition,
                            topic: message.topic
                        }]
                    }
                });
            }

            // get next messages
            emitter.emit("next");
        } catch (e) {
            errorHandler(e);
        }
    };
};

const newConsumer = async (config, eventHandler, errorHandler) => {
    let action = "create handler";
    const processing = handler(config, eventHandler, errorHandler);
    emitter.on("next", processing);
    try {
        action = "create consumer";
        config.kafka.me = (await axios({
            method: "POST",
            url: `http://${config.kafka.api}/consumers/${config.kafka.group_id}`,
            headers: {
                "Content-Type": "application/vnd.kafka.v2+json"
            },
            data: config.kafka.consumer_settings || {
                name: `${config.kafka.group_id}${String(Math.random()).slice(-4)}`,
                "format": "binary",
                "auto.offset.reset": "earliest",
                "auto.commit.enable": "false"
            }
        })).data;

        action = "subscribe";
        await axios({
            method: "POST",
            url: `http://${config.kafka.api}/consumers/${config.kafka.group_id}/instances/${config.kafka.me.instance_id}/subscription`,
            headers: {
                "Content-Type": "application/vnd.kafka.v2+json",
                Host: config.kafka.api
            },
            data: {
                topics: [config.kafka.topic]
            }
        });

        console.log("Subscribed on kafka");
        // get messages
        emitter.emit("next");
    } catch (e) {
        console.log(`Error from rest kafka (${action}): ${e}`);
        throw e;
    }
};

module.exports = newConsumer;
