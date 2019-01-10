"use strict";

const restify = require("restify");

function newClient(url) {
    let client = restify.createJsonClient({
        url,
        version: "*"
    });

    return {
        getImageUrl: file_id => new Promise((resolve, reject) => {
            client.get(`/v1/file/internal/file_url?file_id=${file_id.fileId}`, (err, req, res, obj) => {
                if (err) {
                    return reject(err);
                }
                resolve(obj);
            });
        }),

        internalUploadFile: params => new Promise((resolve, reject) => {
            client.get(`/v1/file/internal/upload_file?mime_type=${params.params.mime_type}&title=${params.params.title}&kind=${params.params.kind}`, (err, req, res, obj) => {
                if (err) {
                    return reject(err);
                }
                resolve(obj);
            });
        })
    };
}

module.exports = newClient;
