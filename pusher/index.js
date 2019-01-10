"use strict";

const restify = require("restify");

function newClient(url) {
    let client = restify.createJsonClient({
        url,
        version: "*"
    });

    return {
        sendEmail: emailMsg => new Promise((resolve, reject) => {
            client.post("/v1/pusher/internal/send_email", emailMsg, (err, req, res, obj) => {
                if (err) {
                    return reject(err);
                }
                resolve(obj);
            });
        })
    };
}

module.exports = newClient;
