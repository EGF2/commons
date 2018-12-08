"use strict";

const restify = require("restify");

function newClient(url) {
    let client = restify.createJsonClient({
        url,
        version: "*"
    });

    let handle = (method, path, body, author) =>
        new Promise((resolve, reject) => {
            let callback = (err, req, res, obj) => {
                if (err) {
                    return reject(err);
                }
                resolve(obj);
            };
            let options = { path };
            if (author) {
                options.headers = {
                    "Author": author
                };
            }

            if (method === "GET") {
                client.get(options, callback);
            } else if (method === "POST") {
                client.post(options, body, callback);
            } else if (method === "PUT") {
                client.put(options, body, callback);
            } else if (method === "PATCH") {
                client.patch(options, body, callback);
            } else if (method === "DELETE") {
                client.del(options, callback);
            }
        });

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
            return handle("GET", "/v1/graph")
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

        /**
         * Get object
         */
        getObject: function (id, options) {
            return handle("GET", `/v1/graph/${id}`).then(result =>
                options && options.expand ? this.expand(result, options.expand) : result
            );
        },

        /**
         * Get objects
         */
        getObjects: function (ids, options) {
            ids = Array.from(arguments);
            if (typeof ids.slice(-1)[0] === "object") {
                options = ids.slice(-1)[0];
                ids = ids.slice(0, -1);
            }
            return handle("GET", `/v1/graph/${ids.join(",")}`).then(result =>
                options && options.expand ? this.expand(result, options.expand) : result
            );
        },

        /**
         * Create object
         */
        createObject: (object, author) => handle("POST", "/v1/graph", object, author),

        /**
         * Update object
         */
        updateObject: (id, delta, author) => handle("PATCH", `/v1/graph/${id}`, delta, author),

        /**
         * Replace object
         */
        replaceObject: (id, object, author) => handle("PUT", `/v1/graph/${id}`, object, author),

        /**
         * Delete object
         */
        deleteObject: (id, author) => handle("DELETE", `/v1/graph/${id}`, undefined, author),

        /**
         * Get edge
         */
        getEdge: function (srcID, edgeName, dstID, options) {
            return handle("GET", `/v1/graph/${srcID}/${edgeName}/${dstID}`).then(result =>
                options && options.expand ? this.expand(result, options.expand) : result
            );
        },

        /**
         * Get edges
         */
        getEdges: function (srcID, edgeName, options) {
            let url = `/v1/graph/${srcID}/${edgeName}`;
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
        createEdge: (srcID, edgeName, dstID, author) => handle("POST", `/v1/graph/${srcID}/${edgeName}/${dstID}`, {}, author),

        /**
         * Delete edge
         */
        deleteEdge: (srcID, edgeName, dstID, author) => handle("DELETE", `/v1/graph/${srcID}/${edgeName}/${dstID}`, undefined, author),

        /**
         * Create audit
         */
        createAudit: (auditLog, author) => handle("POST", "/v1/audit", auditLog, author),

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
