const axios = require("axios");
const { Tags, FORMAT_HTTP_HEADERS } = require("opentracing");

class clientApi {
  constructor(api, tracer) {
    this.api = api;
    this.tracer = tracer;
    this.startTimeout = 250;
    this.deltaInterval = 250;
    this.maxTimeout = 10000;
  }

  async timeout(ms) {
    return new Promise(res => setTimeout(res, ms));
  }

  async request({url, method, body, auth, span, user}) {
    for (
      let waitTime = this.startTimeout;
      waitTime <= this.maxTimeout;
      waitTime += this.deltaInterval
    ) {
      try {
        if (span) span.log({StartReq: url});
        const res = await axios({
          method,
          url: `${this.api}${url}`,
          data: body,
          headers: this.createHeaders({path: url, method, span, auth, user})
        });
        if (span) span.log({EndReq: url});
        return res.data;
      } catch (e) {
        if (e.response && e.response.status >= 500 && e.response.status < 600) await this.timeout(waitTime);
        else throw e;
      }
    }
  }

  createHeaders({auth, span, method, path, user}) {
    const headers = {};
    if (span) {
      span.setTag(Tags.HTTP_URL, path || "");
      span.setTag(Tags.HTTP_METHOD, method || "");
      span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_RPC_CLIENT);
      this.tracer.inject(span, FORMAT_HTTP_HEADERS, headers);
    }
    if (auth) headers.authorization = `Bearer ${auth.split(" ")[1]}`;
    if (user) headers.user = user;
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

  async getObject({id, params, auth, user}) {
    if (!id) throw new Error("'id' is empty");
    let url = `/v2/internal/client-api/graph/${id}`;
    const query = this.prepareQuery(params);
    if (query) url += `?${query}`;
    return this.request({url, method: "GET", span: params.span, auth, user});
  }

  async getEdge({src, name, dst, params, auth, user}) {
    if (!src || !name || !dst) throw new Error("src, dst or name is empty");
    let url = `/v2/internal/client-api/graph/${src}/${name}/${dst}`;
    const query = this.prepareQuery(params);
    if (query) url += `?${query}`;
    return this.request({url, method: "GET", span: params.span, auth, user});
  }

  async getEdges({src, name, params, auth, user}) {
    if (!src || !name) throw new Error("src or name is empty");
    let url = `/v2/internal/client-api/graph/${src}/${name}`;
    const query = this.prepareQuery(params);
    if (query) url += `?${query}`;
    return this.request({url, method: "GET", span: params.span, auth, user});
  }
}

module.exports = clientApi;
