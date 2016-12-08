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
    if (config.queue === "rethinkdb") {
        return require("./rethinkdb")(config, eventHandler, errorHandler);
    }
    throw new Error("Unknown consumer type");
}
module.exports = newConsumer;
