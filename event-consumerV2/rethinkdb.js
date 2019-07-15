"use strict";

const rethinkdb = require("rethinkdbdash");
const SortedSet = require("sorted-set");

/**
 * @param config - rethinkdb config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */
function newConsumer(config, eventHandler, errorHandler) {
    let r = rethinkdb(config.rethinkdb);
    let eventsTable = config.rethinkdb.table || "events";
    let eventOffsetTable = config.rethinkdb.offsettable || "event_offset";
    let eventsSortField = config.rethinkdb.sort_field || "created_at";
    let consumerGroup = config["consumer-group"];

    errorHandler = errorHandler || (() => {});

    function safeHandler(event) {
        return Promise.resolve()
            .then(() => eventHandler(event))
            .catch(errorHandler)
            .then(() => r.table(eventOffsetTable).get(consumerGroup).update({offset: event.id}).run());
    }

    let queue = new SortedSet({
        unique: true,
        hash: function(item) {
            return item.id;
        },
        compare: function(a, b) {
            return a[eventsSortField] - b[eventsSortField];
        }
    });

    /**
     * Select last processed EventOffset object for specified service name
     * @returns {Promise} instance of model.EventOffset
     */
    function getOffset() {
        return r.table(eventOffsetTable).get(consumerGroup).run()
            .then(offset => {
                if (offset) {
                    return offset.offset;
                }
                return r.table(eventOffsetTable)
                    .insert({id: consumerGroup}).run()
                    .then(() => undefined);
            });
    }

    /**
     * Add events to the queue while they are
     * @param offsetId first event id
     * @returns {Promise.<Number>} last event id
     */
    function catchUp(offsetId) {
        let offset;
        if (offsetId) {
            offset = r.table(eventsTable)
                .get(offsetId).run()
                .then(event => event[eventsSortField]);
        } else {
            offset = Promise.resolve();
        }

        return offset
            .then(offset => r.table(eventsTable)
                .between(offset || r.minval, r.maxval,
                    {index: eventsSortField, leftBound: "open", rightBound: "closed"})
                .orderBy({index: eventsSortField}).run()
            )
            .then(events => {
                if (!events.length) {
                    return offsetId;
                }
                let event;
                for (event of events) {
                    try {
                        queue.add(event);
                    } catch (e) {
                        return event.id;
                    }
                }
                catchUp(event.id);
            });
    }

    let seq = Promise.resolve();
    let queueEmpty = false;

    /**
     * Listen events and process them in consecutive order
     */
    function listen() {
        return r.table(eventsTable).changes().run()
            .then(cursor => {
                return cursor.each((error, row) => {
                    if (error) {
                        errorHandler(error);
                        process.exit(101);
                    }
                    let event = row.new_val;
                    if (queueEmpty) {
                        seq = seq.then(() => safeHandler(event));
                    } else {
                        try {
                            queue.add(event);
                        } catch (e) {
                            errorHandler(e);
                        }
                    }
                });
            })
            .catch(error => {
                errorHandler(error);
                process.exit(102);
            });
    }

    function processQueue() {
        queueEmpty = !queue.length;
        if (!queueEmpty) {
            let event = queue.shift();
            return safeHandler(event)
                .then(() => processQueue());
        }
    }

    return listen()
        .then(() => getOffset())
        .then(eventId => catchUp(eventId))
        .then(() => processQueue())
        .catch(err => {
            errorHandler(err);
            throw err;
        });
}
module.exports = newConsumer;
