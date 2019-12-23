const axios = require("axios");
const { Tags, FORMAT_HTTP_HEADERS } = require("opentracing");

class clientApi {
  constructor(api, tracer) {
    this.api = api;
    this.tracer = tracer;
    this.startTimeout = 5;
    this.deltaInterval = 20;
    this.maxTimeout = 3500;
  }

  async timeout(ms) {
    return new Promise(res => setTimeout(res, ms));
  }

  async request({ url, method, body, auth, span }) {
    let err;
    let waitTime = 0;
    const objErr = {};
    for (
      let i = this.startTimeout;
      waitTime <= this.maxTimeout;
      i += this.deltaInterval
    ) {
      try {
        if (span) span.log({ StartReq: url });
        const res = await axios({
          method,
          url: `${this.api}${url}`,
          data: body,
          headers: this.createHeaders({ path: url, method, span, auth })
        });
        if (span) span.log({ EndReq: url });
        return res.data;
      } catch (e) {
        err = e;
        if (!objErr.err)
          objErr.err = { err: e, message: e.message, code: e.code };
        if (!e.message.includes("Gateway")) break;
        await this.timeout(i);
        waitTime += i;
        continue;
      }
    }
    throw err;
  }

  createHeaders({ auth, span, method, path }) {
    const headers = {};
    if (span) {
      span.setTag(Tags.HTTP_URL, path || "");
      span.setTag(Tags.HTTP_METHOD, method || "");
      span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_RPC_CLIENT);
      this.tracer.inject(span, FORMAT_HTTP_HEADERS, headers);
    }
    if (auth) headers.authorization = `Bearer ${auth.split(" ")[1]}`;
    return headers;
  }

  prepareQuery(params) {
    if (!params) return null;
    let query = "";
    if (params.expand) query += `expand=${params.expand}&`;
    if (params.count) query += `count=${params.count}&`;
    if (params.after) query += `after=${params.after}&`;
    return !query.length ? null : query.slice(0, query.length - 1);
  }

  async getObject({ id, params, auth }) {
    if (!id) throw new Error("'id' is empty");
    let url = `/v2/client-api/graph/${id}`;
    const query = this.prepareQuery(params);
  if (query) url += `?${query}`;
    return this.request({ url, method: "GET", span: params.span, auth });
  }

  async getEdge({ src, name, dst, params, auth }) {
    if (!src || !name || !dst) throw new Error("src or dst or name is semty");
    let url = `/v2/client-api/graph/${src}/${name}/${dst}`;
    const query = this.prepareQuery(params);
    if (query) url += `?${query}`;
    return this.request({ url, method: "GET", span: params.span, auth });
  }

  async getEdges({ src, name, params, auth }) {
    if (!src || !name) throw new Error("src or name is semty");
    let url = `/v2/client-api/graph/${src}/${name}`;
    const query = this.prepareQuery(params);
    if (query) url += `?${query}`;
    return this.request({ url, method: "GET", span: params.span, auth });
  }
}

module.exports = clientApi;
