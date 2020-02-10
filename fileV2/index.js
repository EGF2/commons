"use strict";

const restify = require("restify-clients");
const { Tags, FORMAT_HTTP_HEADERS } = require("opentracing");

function newClient(url, tracer) {
  let client = restify.createJsonClient({
    url,
    version: "*"
  });
  const startTimeout = 5;
  const deltaInterval = 20;
  const maxTimeout = 3500;

  const request = (method, options) => {
    return new Promise((resolve, reject) => {
      const callback = (err, req, res, obj) => {
        if (err) {
          return reject(err);
        }
        resolve(obj);
      };
      if (method === "GET") client.get(options, callback);
    });
  };

  const timeout = async ms => {
    return new Promise(res => setTimeout(res, ms));
  };

  const handle = async (method, url, span) => {
    let err;
    let waitTime = 0;
    const objErr = {};
    const options = {
      path: url,
      headers: {}
    };
    if (span) {
      span.setTag(Tags.HTTP_URL, options.path);
      span.setTag(Tags.HTTP_METHOD, method);
      span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_RPC_CLIENT);
      tracer.inject(span, FORMAT_HTTP_HEADERS, options.headers);
    }
    for (let i = startTimeout; waitTime <= maxTimeout; i += deltaInterval) {
      try {
        const res = await request(method, options);
        return res;
      } catch (e) {
        err = e;
        const errors = ["Gateway", "Unavailable"];
        if(!objErr.err) objErr.err = {err: e, message: e.message, code: e.code};
        if (!errors.some(error => e.message.includes(error))) break;
        await timeout(i);
        waitTime += i;
      }
    }
    throw new Error(err);
  };

  return {
    getImageUrl: file_id =>
      handle(
        "GET",
        `/v2/internal/file/file_url?file_id=${file_id.fileId ||
          file_id.id ||
          file_id}`
      ),

    internalUploadFile: params =>
      handle(
        "GET",
        `/v2/internal/file/upload_file?mime_type=${params.params.mime_type}&title=${params.params.title}&kind=${params.params.kind}`
      )
  };
}

module.exports = newClient;
