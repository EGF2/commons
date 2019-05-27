"use strict";

const restify = require("restify");

function newClient(url) {
    let client = restify.createJsonClient({
        url,
        version: "*"
    });
    const startTimeout = 5;
    const deltaInterval = 20;
    const maxTimeout = 3500;

    const request = (method, url) => {
        return new Promise((resolve, reject) => {
            const callback = (err, req, res, obj) => {
                if (err) {
                    return reject(err);
                }
                resolve(obj);
            };
            if (method === "GET") client.get(url, callback);
        });
    };

    const timeout = async ms => {
        return new Promise(res => setTimeout(res, ms));
    };

    const handle = async (method, url) => {
        let err;
        let waitTime = 0;
        const objErr = {};
        for (let i = startTimeout; waitTime <= maxTimeout; i += deltaInterval) {
            try {
                const res = await request(method, url);
                objErr.err && console.log("fileErr", JSON.stringify(objErr));
                return res;
            } catch (e) {
                err = e;
                if(!objErr.err) objErr.err = {err: e, message: e.message, code: e.code}
                if (!e.message.includes("Gateway")) break;
                await timeout(i);
                waitTime += i;
                continue;
            }
        }
        console.log("fileErr", JSON.stringify(objErr));
        throw new Error(err);
    };

    return {
        getImageUrl: file_id => handle("GET", `/v1/internal/file/file_url?file_id=${file_id.fileId}`),

        internalUploadFile: params => handle("GET", `/v1/internal/file/upload_file?mime_type=${params.params.mime_type}&title=${params.params.title}&kind=${params.params.kind}`)
    };
}

module.exports = newClient;
