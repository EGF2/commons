"use strict";

const kafka = require("no-kafka");

/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */
function newConsumer(config, eventHandler, errorHandler) {
    let consumer = new kafka.SimpleConsumer({
        connectionString: config.kafka.hosts.join(","),
        groupId: `${config["consumer-group"]}V2`,
        clientId: config.kafka["client-id"]
    });
    return consumer.init().then(() => {
        return consumer.fetchOffset([{
            topic: config.kafka.topicV2,
            partition: 0
        }])
        .then(fetchOffset => {
            let offset = fetchOffset[0].offset;
            if (offset > -1) {
                offset++;
            }
            consumer.subscribe(config.kafka.topicV2, 0, {offset}, (messageSet, topic, partition) => {
                return Promise.all(messageSet.map(message =>
                    eventHandler(JSON.parse(message.message.value.toString("utf8")))
                ))
                .then(() => {
                    let last = messageSet[messageSet.length - 1];
                    return consumer.commitOffset({
                        topic,
                        partition,
                        offset: last.offset
                    });
                })
                .catch(err => {
                    errorHandler(err);
                });
            });
        })
        .catch(err => {
            errorHandler(err);
            process.exit(101);
        });
    })
    .catch(err => {
        errorHandler(err);
        throw err;
    });
}
module.exports = newConsumer;
