"use strict";

const restify = require("restify-clients");
const { Tags, FORMAT_HTTP_HEADERS } = require("opentracing");

function newClient(url, tracer) {
  let client = restify.createJsonClient({
    url,
    version: "*"
  });
  const startTimeout = 250;
  const deltaInterval = 250;
  const maxTimeout = 10000;

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
    for (let waitTime = startTimeout; waitTime <= maxTimeout; waitTime += deltaInterval) {
      try {
        return request(method, options);
      } catch (e) {
        if (e.response && e.response.status >= 500 && e.response.status < 600) await timeout(waitTime);
        else throw e;
      }
    }
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
