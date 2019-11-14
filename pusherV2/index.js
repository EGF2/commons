"use strict";

const restify = require("restify-clients");

function newClient(url) {
    let client = restify.createJsonClient({
        url,
        version: "*"
    });

    return {
        sendEmail: emailMsg => new Promise((resolve, reject) => {
            client.post("/v2/internal/pusher/send_email", emailMsg, (err, req, res, obj) => {
                if (err) {
                    return reject(err);
                }
                resolve(obj);
            });
        }),

        /**
         * @temaplateName - name template sms,
         * @to - to number
         *
         * @reqReport - id reqReport,
         * or
         * @code - number code
         * */
        sendSms: params => new Promise((resolve, reject) => {
            client.post("/v2/internal/pusher/send_sms", params, (err, req, res, obj) => {
                if (err) {
                    return reject(err);
                }
                resolve(obj);
            })
        })
    };
}

module.exports = newClient;
