"use strict";

const restify = require("restify-clients");
const { Tags, FORMAT_HTTP_HEADERS } = require("opentracing");
const axios = require("axios");
let _url;

function newClient(url, mode, tracer) {
    _url = url;
    // const client = restify.createJsonClient({
    //     url,
    //     version: "*"
    // });

    let ignoreErrors;

    if (mode && mode.ignoreCD) ignoreErrors = mode.ignoreCD;

    // Time in ms
    const startTimeout = 5;
    const deltaInterval = 20;
    const maxTimeout = 3500;

    const request = async (method, params) => {
        const res = await axios({
            method,
            url: `${_url}${params.path}`,
            headers: params.headers || {},
            data: params.body || {}
        });
        return res.data;

        // return new Promise((resolve, reject) => {
        //     const callback = (err, req, res, obj) => {
        //         if (err) return reject(err);
        //         resolve(obj);
        //     };
        //     if (method === "GET") {
        //         client.get(params.options, callback);
        //     } else if (method === "POST") {
        //         client.post(params.options, params.body, callback);
        //     } else if (method === "PUT") {
        //         client.put(params.options, params.body, callback);
        //     } else if (method === "PATCH") {
        //         client.patch(params.options, params.body, callback);
        //     } else if (method === "DELETE") {
        //         client.del(params.options, callback);
        //     }
        // });
    };

    const timeout = async ms => new Promise(res => setTimeout(res, ms));

    const handle = async (option, method, path, body, author, notProcess) => {
        const options = {
            path,
            headers: {}
        };
        let span = {
            log: () => {},
            setTag: () => {},
        };

        if (option && option.span) {
            span = option.span;
            span.setTag(Tags.HTTP_URL, path);
            span.setTag(Tags.HTTP_METHOD, method);
            span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_RPC_CLIENT);
            tracer.inject(span, FORMAT_HTTP_HEADERS, options.headers);
        }
        span.log({message: "start handler"});
        if (mode && mode.service) options.headers.service = mode.service;
        if (author) options.headers.Author = author;
        if (notProcess) options.headers.notProcess = notProcess;

        let err;
        let waitTime = 0;
        const objErr = {};
        for (let i = startTimeout; waitTime <= maxTimeout; i += deltaInterval) {
            let res;
            try {
                span.log({message: "start request"});
                res = await request(method, { options, body });
                span.log({message: "recived result"});
                return res;
            } catch (e) {
                span.log({message: "recived error"});
                err = e;
                if (!objErr.err) objErr.err = { err: e, message: e.message, code: e.code }
                if (ignoreErrors) {
                    if ((e.body && e.body.code === "ObjectDeleted") || e.message.includes("ObjectDeleted")) return { message: new Date().toISOString() };
                    if ((e.body && e.body.code === "SourceWasDeleted") || e.message.includes("SourceWasDeleted")) return { message: new Date().toISOString() };
                    if ((e.body && e.body.code === "EdgeNotExists") || e.message.includes("EdgeNotExists")) return { message: new Date().toISOString() };
                    if ((e.body && e.body.code === "EdgeAlreadyExists") || e.message.includes("EdgeAlreadyExists")) return { message: new Date().toISOString() };
                }
                if (!e.message.includes("Gateway")) break;
                await timeout(i);
                waitTime += i;
                continue;
            }
        }
        if (span) {
            span.setTag(Tags.ERROR, true);
            span.setTag(Tags.HTTP_STATUS_CODE, err.statusCode);
            span.log({
                event: "error",
                message: err.message,
                err
            });
        }

        throw err;
    };

    // keep config
    let graphConfig;
    let aggregatesConfig;

    // keep object code to object type map
    let codeToObjectType;

    return {
        /**
         * Get graph config
         */
        getGraphConfig: function (options) {
            if (graphConfig) {
                return Promise.resolve(graphConfig);
            }
            return handle(options, "GET", "/v2/client-data/graph")
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

        getAggregatesConfig: function (options) {
            if (aggregatesConfig) {
                return Promise.resolve(aggregatesConfig);
            }
            return handle(options, "GET", "/v2/client-data/aggregates")
                .then(result => {
                    aggregatesConfig = result;
                    return aggregatesConfig;
                });
        },
        /**
         * Get edge config
         */
        getEdgeConfig: function (objOrID, edgeName, options) {
            return this.getGraphConfig(options).then(config => {
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

        /*
         * Get aggregate
        */
        getAggregate: (id, aggregate, options, author, v) =>
            handle(options, "GET", `/v2/client-data/aggregate/${id}?aggregate=${aggregate}&v${v}`, null, author),

        /**
         * Get object type by ID
         */
        getObjectType: function (id, options) {
            return this.getGraphConfig(options).then(() => codeToObjectType[id.slice(-2)]);
        },

        resolveUniqueCode: (code, options) => handle(options, "GET", `/v2/client-data/resolve_unique/${code}`),

        /**
         * Get object
         */
        getObject: function (id, options, author) {
            return handle(options, "GET", `/v2/client-data/graph/${id}`, "", author).then(result =>
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
            return handle(options, "GET", `/v2/client-data/graph/${ids.join(",")}`, "", author).then(result =>
                options && options.expand ? this.expand(result, options.expand) : result
            );
        },

        /**
         * Create object
         */
        createObject: (object, options, author, notProcess) => handle(options, "POST", "/v2/client-data/graph", object, author, notProcess),

        /**
         * Update object
         */
        updateObject: (id, delta, options, author, notProcess) => handle(options, "PATCH", `/v2/client-data/graph/${id}`, delta, author, notProcess),

        /**
         * Replace object
         */
        replaceObject: (id, object, options, author, notProcess) => handle(options, "PUT", `/v2/client-data/graph/${id}`, object, author, notProcess),

        /**
         * Delete object
         */
        deleteObject: (id, options, author, notProcess) => handle(options, "DELETE", `/v2/client-data/graph/${id}`, undefined, author, notProcess),

        /**
         * Get edge
         */
        getEdge: function (srcID, edgeName, dstID, options) {
            return handle(options, "GET", `/v2/client-data/graph/${srcID}/${edgeName}/${dstID}`).then(result =>
                options && options.expand ? this.expand(result, options.expand) : result
            );
        },

        /**
         * Get all edges
         */
        getAllEdges: (srcID, edgeName, options, v) => handle(options, "GET", `/v2/client-data/getAllEdges/${srcID}/${edgeName}?v=${v}`),

        /**
         * Get edges
         */
        // eslint-disable-next-line object-shorthand
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
            return handle(options, "GET", url).then(result =>
                options && options.expand ? this.expand(result, options.expand) : result
            );
        },

        /**
         * Create edge
         */
        createEdge: (srcID, edgeName, dstID, options, author, notProcess) =>
            handle(options, "POST", `/v2/client-data/graph/${srcID}/${edgeName}/${dstID}`, {}, author, notProcess),

        /**
         * Delete edge
         */
        deleteEdge: (srcID, edgeName, dstID, options, author, notProcess) =>
            handle(options, "DELETE", `/v2/client-data/graph/${srcID}/${edgeName}/${dstID}`, undefined, author, notProcess),

        /**
         * Create audit
         */
        createAudit: (auditLog, options, author) =>
            handle(options, "POST", "/v2/client-data/audit", auditLog, author),

        /**
         * Check unique value
         */
        checkUnique: (value, options) =>
            handle(options, "GET", `/v2/client-data/check_unique?value=${value}`),

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
