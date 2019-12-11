"use strict";

const restify = require("restify-clients");
const errors = require("restify-errors");
const { Tags, FORMAT_HTTP_HEADERS } = require("opentracing");

// Authorization header
const AUTH = "authorization";
// query parameter
const TOKEN = "token";

function _getToken(req) {
  let token;
  if (
    req.headers &&
    AUTH in req.headers &&
    req.headers[AUTH].startsWith("Bearer ")
  ) {
    // 1. get token from header and remove 'Bearer ' prefix
    token = req.headers[AUTH].substr(7);
  } else if (req.query && TOKEN in req.query) {
    // 2. get token from query
    token = req.query[TOKEN];
  }

  if (token) {
    return token;
  }

  // throw unauthorization exception
  throw new errors.UnauthorizedError("Bearer token doesn't exist");
}

class Client {
  constructor(url, tracer) {
    this.client = restify.createJsonClient({
      url: url,
      version: "*"
    });
    this.tracer = tracer;
  }

  /**
   * Get auth token from request:
   * 1. Check Authorization header (extract Bearer token)
   * 2. Check token parameter in query
   */
  getToken(req) {
    return _getToken(req);
  }

  /**
   * Check token in Auth server
   * @param auth - url to auth server (<schema>://<host>:<port>)
   * @param reqOrToken - request object or token
   */
  checkToken(reqOrToken, span) {
    let client = this.client;
    return new Promise((resolve, reject) => {
      let token;

      if (typeof reqOrToken === "string") {
        token = reqOrToken;
      } else {
        token = _getToken(reqOrToken);
      }
      const params = {
        path: `/v2/internal/auth/session?token=${token}`,
        headers: {}
      };
      if (span) {
        span.setTag(Tags.HTTP_URL, params.path);
        span.setTag(Tags.HTTP_METHOD, "GET");
        span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_RPC_CLIENT);
        this.tracer.inject(span, FORMAT_HTTP_HEADERS, params.headers);
      }
      client.get(params, (err, req, res, obj) => {
        if (err) {
          return reject(err);
        }
        if (!obj) {
          return reject(new errors.UnauthorizedError("Token not found"));
        }
        if (obj.deleted_at) {
          return reject(
            new errors.UnauthorizedError("Bearer token doesn't exist")
          );
        }
        resolve(obj);
      });
    });
  }

  register(params, span) {
    let client = this.client;
    const options = {
      path: "/v2/internal/auth/register",
      headers: {}
    };
    if (span) {
      span.setTag(Tags.HTTP_URL, params.path);
      span.setTag(Tags.HTTP_METHOD, "GET");
      span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_RPC_CLIENT);
      this.tracer.inject(span, FORMAT_HTTP_HEADERS, options.headers);
    }
    return new Promise((resolve, reject) => {
      client.post(options, params, (err, req, res, obj) => {
        if (err) {
          return reject(err);
        }
        resolve(obj);
      });
    });
  }
}
module.exports.Client = Client;

/**
 * Middleware for handle token from request
 * @param url - url to Auth server
 * @param allowPublicAccess - function which take request and return promise of bool value
 */
function handler(url, allowPublicAccess, tracer) {
  let client = new Client(url, tracer);
  return async function(req, res, next) {
    let span;
    if (req.span.fake) {
      span = {
        setTag: () => {},
        log: () => {},
        finish: () => {},
      };
    } else {
      span = tracer.startSpan("checkAuth", {
        childOf: req.span,
        tags: { session: req.session, function: "checkAuth" }
      });
    }

    span.log({ params: req.params, path: req.path() });
    try {
      const session = await client.checkToken(req, span);
      req.session = session; // set session to request
      if (session.user) {
        req.user = session.user; // set user to request
      }
      span.log({ message: "Access is allowed " });
      span.finish();
      return next();
    } catch (e) {
      span.log({ event: "Error checkToken. Check allowPublicAccess" });
      if (allowPublicAccess) {
        try {
          if (await allowPublicAccess(req, span)) {
            span.log({ message: "Access is allowed " });
            span.finish();
            return next();
          }
        } catch (e) {
          span.log({
            event: "Error allowPublicAccess. Access denied",
            error: e.message
          });
          span.finish();
          return next(e);
        }
      }
      span.log({ event: "Error checkToken. Access denied" });
      span.finish();
      return next(e);
    }
  };
}
module.exports = {
  Client,
  handler,
  getToken: _getToken
};
