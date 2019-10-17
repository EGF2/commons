"use strict";

/**
 * Create new consumer for events
 * @param config - contains name and consumer config. Example:
 *                 {name: "Consumer1", rethinkdb: {db: "eigengraph"}}
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 * @return Promise with event consumer
 */
const producerTypes = {
    kafka: require("./kafka")
};

function newProducer(config) {
    if (!producerTypes[config.queue]) {
        throw new Error(`Not supported producer type at config.queue: ${config.queue}. Please use one of the following: [${Object.keys(producerTypes).join(", ")}]`);
    }
    return producerTypes[config.queue](config);
}

module.exports = newProducer;
