"use strict";

const restify = require("restify-clients");
const errors = require("restify-errors");

// Authorization header
const AUTH = "authorization";
// query parameter
const TOKEN = "token";

function _getToken(req) {
    let token;
    if (req.headers && AUTH in req.headers && req.headers[AUTH].startsWith("Bearer ")) {
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
    constructor(url) {
        this.client = restify.createJsonClient({
            url: url,
            version: "*"
        });
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
    checkToken(reqOrToken) {
        let client = this.client;
        return new Promise((resolve, reject) => {
            let token;

            if (typeof reqOrToken === "string") {
                token = reqOrToken;
            } else {
                token = _getToken(reqOrToken);
            }

            client.get(`/v2/internal/auth/session?token=${token}`, (err, req, res, obj) => {
                if (err) {
                    return reject(err);
                }
                if (!obj) {
                    return reject(new errors.UnauthorizedError("Token not found"));
                }
                if (obj.deleted_at) {
                    return reject(new errors.UnauthorizedError("Bearer token doesn't exist"));
                }
                resolve(obj);
            });
        });
    }

    register(params) {
        let client = this.client;
        return new Promise((resolve, reject) => {
            client.post("/v2/internal/auth/register", params, (err, req, res, obj) => {
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
function handler(url, allowPublicAccess) {
    let client = new Client(url);
    return function(req, res, next) {
        client.checkToken(req).then(session => {
            req.session = session; // set session to request
            if (session.user) {
                req.user = session.user; // set user to request
            }
            next();
        }).catch(err => {
            if (allowPublicAccess) {
                return allowPublicAccess(req).then(ok => {
                    if (ok) {
                        return next();
                    }
                    return next(err);
                }).catch(err => next(err));
            }
            next(err);
        });
    };
}
module.exports = {
    Client,
    handler,
    getToken: _getToken
};
