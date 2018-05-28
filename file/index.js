"use strict";

const restify = require("restify");

function newClient(url) {
    let client = restify.createJsonClient({
        url,
        version: "*"
    });

    return {
        getImageUrl: file_id => new Promise((resolve, reject) => {
            client.get("/v1/internal/file_url", file_id, (err, req, res, obj) => {
                if (err) {
                    return reject(err);
                }
                resolve(obj);
            });
        })
    };
}

module.exports = newClient;
