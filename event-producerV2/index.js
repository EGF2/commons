"use strict";

/**
 * Create new events producer
 * @param config - contains name and consumer config.
 * @return event producer function
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
