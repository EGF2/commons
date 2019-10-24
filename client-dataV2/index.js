"use strict";

const restify = require("restify-clients");

function newClient(url, mode) {

    let client = restify.createJsonClient({
        url,
        version: "*"
    });

    let ignoreErrors;

    if (mode && mode.ignoreCD) ignoreErrors = mode.ignoreCD;

    // Time in ms
    const startTimeout = 5;
    const deltaInterval = 20;
    const maxTimeout = 3500;

    let request = (method, params) => {
        return new Promise((resolve, reject) => {
            let callback = (err, req, res, obj) => {
                if (err) {
                    return reject(err);
                }
                resolve(obj);
            };
            if (method === "GET") {
                client.get(params.options, callback);
            } else if (method === "POST") {
                client.post(params.options, params.body, callback);
            } else if (method === "PUT") {
                client.put(params.options, params.body, callback);
            } else if (method === "PATCH") {
                client.patch(params.options, params.body, callback);
            } else if (method === "DELETE") {
                client.del(params.options, callback);
            }
        });
    };

    const timeout = async ms => {
        return new Promise(res => setTimeout(res, ms));
    };

    const handle = async (method, path, body, author, notProcess) => {
        let options = {
            path,
            headers: {}
        };
        if (mode && mode.service) options.headers.service = mode.service;
        if (author) options.headers.Author = author;
        if (notProcess) options.headers.notProcess = notProcess;
        let err;
        let waitTime = 0;
        const objErr = {};
        for (let i = startTimeout; waitTime <= maxTimeout; i += deltaInterval) {
            let res;
            try {
                res = await request(method, { options, body });
                return res;
            } catch (e) {
                err = e;
                if (!objErr.err) objErr.err = { err: e, message: e.message, code: e.code }
                if(ignoreErrors) {
                    if ((e.body && e.body.code === "ObjectDeleted") || e.message.includes("ObjectDeleted")) return {message: new Date().toISOString()};
                    if ((e.body && e.body.code === "SourceWasDeleted") || e.message.includes("SourceWasDeleted")) return {message: new Date().toISOString()};
                    if ((e.body && e.body.code === "EdgeNotExists") || e.message.includes("EdgeNotExists")) return {message: new Date().toISOString()};
                    if ((e.body && e.body.code === "EdgeAlreadyExists") || e.message.includes("EdgeAlreadyExists")) return {message: new Date().toISOString()};
                }
                if (!e.message.includes("Gateway")) break;
                await timeout(i);
                waitTime += i;
                continue;
            }
        }
        throw err;
    };

    // keep graph config
    let graphConfig;

    // keep object code to object type map
    let codeToObjectType;

    return {
        /**
         * Get graph config
         */
        getGraphConfig: function () {
            if (graphConfig) {
                return Promise.resolve(graphConfig);
            }
            return handle("GET", "/v2/client-data/graph")
                .then(result => {
                    graphConfig = result;
                    codeToObjectType = {};
                    Object.keys(graphConfig).forEach(objectType => {
                        if (graphConfig[objectType].code) {
                            codeToObjectType[graphConfig[objectType].code] = objectType;
                        }
                    });
                    return graphConfig;
                });
        },

        /**
         * Get edge config
         */
        getEdgeConfig: function (objOrID, edgeName) {
            return this.getGraphConfig().then(config => {
                let objectType;
                if (typeof objOrID === "string") {
                    if (objOrID in config) {
                        objectType = objOrID;
                    } else {
                        objectType = codeToObjectType[objOrID.slice(-2)];
                    }
                } else {
                    objectType = objOrID.object_type;
                }
                config = config[objectType];
                if (!config) {
                    throw Error(`Unknown object type '${objectType}'`);
                }
                if (!(edgeName in config.edges)) {
                    throw Error(`Unknown edge for '${objectType}/${edgeName}'`);
                }
                return config.edges[edgeName];
            });
        },

        /**
         * Get object type by ID
         */
        getObjectType: function (id) {
            return this.getGraphConfig().then(() => codeToObjectType[id.slice(-2)]);
        },

        /*
         * Get aggregate
        */
        getAggregate: (id, aggregate, author) => handle("GET", `/v2/client-data/aggregate/${id}?aggregate=${aggregate}`,null ,author),

        /**
         * Get object
         */
        getObject: function (id, options, author) {
            return handle("GET", `/v2/client-data/graph/${id}`, "", author).then(result =>
                options && options.expand ? this.expand(result, options.expand) : result
            );
        },

        /**
         * Get objects
         */
        getObjects: function (ids, options, author) {
            if (typeof ids.slice(-1)[0] === "object") {
                options = ids.slice(-1)[0];
                ids = ids.slice(0, -1);
            }
            return handle("GET", `/v2/client-data/graph/${ids.join(",")}`, "", author).then(result =>
                options && options.expand ? this.expand(result, options.expand) : result
            );
        },

        /**
         * Create object
         */
        createObject: (object, author, notProcess) => handle("POST", "/v2/client-data/graph", object, author, notProcess),

        /**
         * Update object
         */
        updateObject: (id, delta, author, notProcess) => handle("PATCH", `/v2/client-data/graph/${id}`, delta, author, notProcess),

        /**
         * Replace object
         */
        replaceObject: (id, object, author, notProcess) => handle("PUT", `/v2/client-data/graph/${id}`, object, author, notProcess),

        /**
         * Delete object
         */
        deleteObject: (id, author, notProcess) => handle("DELETE", `/v2/client-data/graph/${id}`, undefined, author, notProcess),

        /**
         * Get edge
         */
        getEdge: function (srcID, edgeName, dstID, options) {
            return handle("GET", `/v2/client-data/graph/${srcID}/${edgeName}/${dstID}`).then(result =>
                options && options.expand ? this.expand(result, options.expand) : result
            );
        },

        /**
         * Get edges
         */
        getEdges: function (srcID, edgeName, options) {
            let url = `/v2/client-data/graph/${srcID}/${edgeName}`;
            if (options) {
                let params = [];
                if (options.after !== undefined) {
                    params.push(`after=${options.after}`);
                }
                if (options.count !== undefined) {
                    params.push(`count=${options.count}`);
                }
                if (params.length) {
                    url += `?${params.join("&")}`;
                }
            }
            return handle("GET", url).then(result =>
                options && options.expand ? this.expand(result, options.expand) : result
            );
        },

        /**
         * Create edge
         */
        createEdge: (srcID, edgeName, dstID, author, notProcess) => handle("POST", `/v2/client-data/graph/${srcID}/${edgeName}/${dstID}`, {}, author, notProcess),

        /**
         * Delete edge
         */
        deleteEdge: (srcID, edgeName, dstID, author, notProcess) => handle("DELETE", `/v2/client-data/graph/${srcID}/${edgeName}/${dstID}`, undefined, author, notProcess),

        /**
         * Create audit
         */
        createAudit: (auditLog, author) => handle("POST", "/v2/client-data/audit", auditLog, author),

        /**
         * Handle all pages
         * @param {function(last) Promise<page>} query - execute query to server
         * @param {function(page) Promise} handler - handle each page
         */
        forEachPage: (query, handler) => {
            let count = 0;
            let action = page =>
                Promise.resolve().then(() => {
                    return Promise.resolve().then(() => handler(page))
                        .then(() => {
                            count += page.results.length;
                            if (page.last && count && count < page.count) {
                                return query(page.last).then(action);
                            }
                        });
                });
            return query().then(action);
        },

        /**
         * Expand object or page results with expand
         */
        expand: function (object, expand) {
            // array of objects which need expand
            let objects = [];
            if (object.results || object.results === []) {
                objects = object.results;
            } else {
                objects.push(object);
            }
            // "field or edge" -> "next level expand"
            let expandMap = {};
            let curly = 0; // count of curly brackets
            let current = ""; // current expand item
            expand.split("").forEach(ch => {
                switch (ch) {
                    case ",":
                        if (curly === 0) {
                            expandMap[current] = "";
                            current = "";
                        } else {
                            current += ch;
                        }
                        break;
                    case "{":
                        current += ch;
                        curly++;
                        break;
                    case "}":
                        current += ch;
                        curly--;
                        break;
                    default:
                        current += ch;
                }
            });
            if (current) {
                expandMap[current] = "";
            }
            Object.keys(expandMap).forEach(key => {
                let i = key.search("{");
                if (i > 0) {
                    delete expandMap[key];
                    expandMap[key.slice(0, i)] = key.slice(i + 1, -1);
                }
            });

            // apply expandMap to objects
            if (objects && objects.length) {
                objects = objects.map(obj => {
                    return this.getGraphConfig().then(config => {
                        let objCfg = config[obj.object_type]; // object config
                        let promises = [];
                        Object.keys(expandMap).forEach(field => {
                            let options = {
                                expand: expandMap[field]
                            };
                            let split = field.split("(");
                            field = split[0];
                            if (split[1]) {
                                options.count = split[1].slice(0, -1);
                            }
                            let promise;
                            if (field in obj && field in objCfg.fields) {
                                if (!obj[field]) return;
                                promise = this.getObject(obj[field], options);
                            } else if (objCfg.edges && field in objCfg.edges) {
                                promise = this.getEdges(obj.id, field, options);
                            }
                            if (promise) {
                                promises.push(promise.then(res => {
                                    obj[field] = res;
                                }));
                            }
                        });
                        return Promise.all(promises);
                    });
                });
            }
            return Promise.all(objects).then(() => object);
        }
    };
}

module.exports = newClient;
