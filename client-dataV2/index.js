

const restify = require("restify-clients");
const { Tags, FORMAT_HTTP_HEADERS } = require("opentracing");
const axios = require("axios");
const redis = require("redis");
const { promisify } = require("util");
const Logging = require("../Logging");

const Log = new Logging(__filename);

let _url;

function newClient(url, mode, tracer) {
  _url = url;
  const client = restify.createJsonClient({
    url,
    version: "*"
  });
  // connect to redis
  const options = mode.redis.split(":");
  const redisClient = redis.createClient({
    host: options[0],
    port: options[1]
  });
  const redisGet = promisify(redisClient.get).bind(redisClient);

  redisClient.on("error", err => {
    Log.error("Error: ", err);
  });

  redisClient.on("connect", () => {
    Log.info("Connect to REDIS");
  });

  let ignoreErrors;

  if (mode && mode.ignoreCD) ignoreErrors = mode.ignoreCD;

  // Time in ms
  const startTimeout = 250;
  const deltaInterval = 250;
  const maxTimeout = 10000;

  const request = async (method, params) => {
    //     const res = await axios({
    //         method,
    //         url: `${_url}${params.options.path}`,
    //         headers: params.headers || {},
    //         data: params.options.body || {}
    //     });
    //     return res.data;

    return new Promise((resolve, reject) => {
      const callback = (err, req, res, obj) => {
        if (err) return reject(err);
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

  const timeout = async ms => new Promise(res => setTimeout(res, ms));

  const handle = async (option, method, path, body, author, notProcess) => {
    const options = {
      path,
      headers: {}
    };
    let span = {
      log: () => { },
      setTag: () => { }
    };

    if (option && option.span) {
      span = option.span;
      span.setTag(Tags.HTTP_URL, path);
      span.setTag(Tags.HTTP_METHOD, method);
      span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_RPC_CLIENT);
      tracer.inject(span, FORMAT_HTTP_HEADERS, options.headers);
    }
    span.log({ message: "start handler" });
    if (mode && mode.service) options.headers.service = mode.service;
    if (author) options.headers.Author = author;
    if (notProcess) options.headers.notProcess = notProcess;

    let err;
    const waitTime = 0;
    for (
      let waitTime = startTimeout;
      waitTime <= maxTimeout;
      waitTime += deltaInterval
    ) {
      let res;
      try {
        span.log({ message: "start request" });
        res = await request(method, { options, body });
        span.log({ message: "received result" });
        return res;
      } catch (e) {
        span.log({ message: "received error" });
        err = e;
        if (ignoreErrors) {
          if (
            (e.body && e.body.code === "ObjectDeleted") ||
            e.message.includes("ObjectDeleted")
          )
            return { message: new Date().toISOString() };
          if (
            (e.body && e.body.code === "SourceWasDeleted") ||
            e.message.includes("SourceWasDeleted")
          )
            return { message: new Date().toISOString() };
          if (
            (e.body && e.body.code === "EdgeNotExists") ||
            e.message.includes("EdgeNotExists")
          )
            return { message: new Date().toISOString() };
          if (
            (e.body && e.body.code === "EdgeAlreadyExists") ||
            e.message.includes("EdgeAlreadyExists")
          )
            return { message: new Date().toISOString() };
        }
        if (e.response && e.response.status >= 500 && e.response.status < 600)
          await timeout(waitTime);
        else break;
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
    console.log(
      JSON.stringify({
        name: "test service",
        m1: err.message,
        m2: err.response && err.response.data
      })
    );
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
    getGraphConfig(options) {
      if (graphConfig) {
        return Promise.resolve(graphConfig);
      }
      return handle(options, "GET", "/v2/client-data/graph").then(result => {
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

    getAggregatesConfig(options) {
      if (aggregatesConfig) {
        return Promise.resolve(aggregatesConfig);
      }
      return handle(options, "GET", "/v2/client-data/aggregates").then(
        result => {
          aggregatesConfig = result;
          return aggregatesConfig;
        }
      );
    },
    /**
     * Get edge config
     */
    getEdgeConfig(objOrID, edgeName, options) {
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
    getAggregate: (id, aggregate, options, author) =>
      handle(
        options,
        "GET",
        `/v2/client-data/aggregate/${id}?aggregate=${aggregate}`,
        null,
        author
      ),

    /**
     * Get object type by ID
     */
    getObjectType(id, options) {
      return this.getGraphConfig(options).then(
        () => codeToObjectType[id.slice(-2)]
      );
    },

    resolveUniqueCode: (code, options) =>
      handle(options, "GET", `/v2/client-data/resolve_unique/${code}`),

    /**
     * Get object
     */
    async getObject(id, options, author) {
      let object = null;
      try {
        object = await redisGet(id);
      } catch (error) {
        Log.warning("Redis request fail", {
          id,
          message: error.message,
        });
      }

      if (object) {
        object = JSON.parse(object);
      } else {
        object = await handle(
          options,
          "GET",
          `/v2/client-data/graph/${id}`,
          "",
          author
        );
      }

      if (object && options && options.expand)
        object = await this.expand(object, options.expand);

      return object;
    },

    /**
     * Get objects
     */
    getObjects(ids, options, author) {
      if (typeof ids.slice(-1)[0] === "object") {
        options = ids.slice(-1)[0];
        ids = ids.slice(0, -1);
      }
      return handle(
        options,
        "GET",
        `/v2/client-data/graph/${ids.join(",")}`,
        "",
        author
      ).then(result =>
        options && options.expand ? this.expand(result, options.expand) : result
      );
    },

    /**
     * Create object
     */
    createObject: (object, options, author, notProcess) =>
      handle(
        options,
        "POST",
        "/v2/client-data/graph",
        object,
        author,
        notProcess
      ),

    /**
     * Update object
     */
    updateObject: (id, delta, options, author, notProcess) =>
      handle(
        options,
        "PATCH",
        `/v2/client-data/graph/${id}`,
        delta,
        author,
        notProcess
      ),

    /**
     * Replace object
     */
    replaceObject: (id, object, options, author, notProcess) =>
      handle(
        options,
        "PUT",
        `/v2/client-data/graph/${id}`,
        object,
        author,
        notProcess
      ),

    /**
     * Delete object
     */
    deleteObject: (id, options, author, notProcess) =>
      handle(
        options,
        "DELETE",
        `/v2/client-data/graph/${id}`,
        undefined,
        author,
        notProcess
      ),

    /**
     * Get edge
     */
    async getEdge(src, name, dst, options) {
      // from redis
      const response = await redisGet(`${src}-${name}`);
      if (response) {
        const edges = JSON.parse(response);
        const id = edges.find(e => e === dst);
        if (id) return this.getObject(id, options);
      }

      // from client-data
      const result = await handle(
        options,
        "GET",
        `/v2/client-data/graph/${src}/${name}/${dst}`
      )
      return options && options.expand ? this.expand(result, options.expand) : result
    },

    /**
     * Get all edges
     * @param {String} src source object id
     * @param {String} name edge name
     * @param {Object} options options, for example: { expand: "field, ..." }
     * @return {Array} objects on edge
     */
    async getAllEdges(src, name, options) {
      // from redis
      const response = await redisGet(`${src}-${name}`);
      if (response) {
        const ids = JSON.parse(response);
        try {
          // If the radish is an empty array then the array of objects will throw an error
          // getObjects can return an object instead of an array if the length of the array with ID equal to 1
          let results;
          if (ids.length) {
            results = await this.getObjects(ids, options);
            results = ids.length === 1 ? [results] : results.results;
          } else results = [];

          return results;
        } catch (e) {
          throw e;
        }
      }

      // from client-data
      const result = await handle(
        options,
        "GET",
        `/v2/client-data/getAllEdges/${src}/${name}`
      )
      return options && options.expand ? this.expand(result, options.expand) : result
    },

    /**
     * Get edges
     * @param {String} src source object id
     * @param {String} name edge name
     * @param {Object} options options, for example: { expand: "field, ..." }
     * @return {Object} with fields results and count
     */
    async getEdges(src, name, options = {}) {
      // from redis
      const response = await redisGet(`${src}-${name}`);
      if (response) {
        const edges = JSON.parse(response)
        const countEdges = edges.length;
        const after = options.after || 0;

        // Get pagination constants from graph config
        const { pagination } = await this.getGraphConfig()
        const count = options.count
          ? options.count > pagination.max_count ? pagination.max_count : options.count // max 100
          : pagination.default_count; // default 25
        const ids = edges.splice(after, count);

        // If the radish is an empty array then the array of objects will throw an error
        // getObjects can return an object instead of an array if the length of the array with ID equal to 1
        let results;
        if (ids.length) {
          results = await this.getObjects(ids, options);
          results = ids.length === 1 ? [results] : results.results;
        } else results = [];
        return {
          results,
          count: countEdges,
        }
      }

      // from client-data
      let url = `/v2/client-data/graph/${src}/${name}`;
      if (options) {
        const params = [];
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
      const result = await handle(options, "GET", url)
      return options && options.expand ? this.expand(result, options.expand) : result
    },

    /**
     * Create edge
     */
    createEdge: (srcID, edgeName, dstID, options, author, notProcess) =>
      handle(
        options,
        "POST",
        `/v2/client-data/graph/${srcID}/${edgeName}/${dstID}`,
        {},
        author,
        notProcess
      ),

    /**
     * Delete edge
     */
    deleteEdge: (srcID, edgeName, dstID, options, author, notProcess) =>
      handle(
        options,
        "DELETE",
        `/v2/client-data/graph/${srcID}/${edgeName}/${dstID}`,
        undefined,
        author,
        notProcess
      ),

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
      const action = page =>
        Promise.resolve().then(() => {
          return Promise.resolve()
            .then(() => handler(page))
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
    expand(object, expand) {
      // array of objects which need expand
      let objects = [];
      if (object.results || object.results === []) {
        objects = object.results;
      } else {
        objects.push(object);
      }
      // "field or edge" -> "next level expand"
      const expandMap = {};
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
        const i = key.search("{");
        if (i > 0) {
          delete expandMap[key];
          expandMap[key.slice(0, i)] = key.slice(i + 1, -1);
        }
      });

      // apply expandMap to objects
      if (objects && objects.length) {
        objects = objects.map(obj => {
          return this.getGraphConfig().then(config => {
            const objCfg = config[obj.object_type]; // object config
            const promises = [];
            Object.keys(expandMap).forEach(field => {
              const options = {
                expand: expandMap[field]
              };
              const split = field.split("(");
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
                promises.push(
                  promise.then(res => {
                    obj[field] = res;
                  })
                );
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
