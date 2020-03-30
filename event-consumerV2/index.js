"use strict";
/**
 * Create new consumer for events
 * @param config - contains name and consumer config. Example:
 *                 {name: "Consumer1", rethinkdb: {db: "eigengraph"}}
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 * @return Promise with event consumer
 */
function newConsumer(config, eventHandler, errorHandler) {
    switch (config.queue) {
        case "kafka": return require("./kafka")(config, eventHandler, errorHandler);
        case "rethinkdb": return require("./rethinkdb")(config, eventHandler, errorHandler);
        case "kinesis": return require("./kinesis")(config, eventHandler, errorHandler);
        case "errorFile": return require("./errorFile")(config, eventHandler, errorHandler);
        default: throw new Error("Unknown consumer type");
    }
}
module.exports = newConsumer;