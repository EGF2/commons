"use strict";

const request = require("request");
const fs = require("fs");
const yargs = require("yargs");

function readOptions(options) {
    options = Object.assign({}, options);
    options.c = {
        alias: "config",
        demand: true,
        describe: "url or path to config file",
        type: "string",
        coerce: path => new Promise((resolve, reject) => {
            if (path.startsWith("http")) {
                return request(path, (err, resp, body) => {
                    if (err) {
                        return reject(err);
                    }
                    resolve(JSON.parse(body));
                });
            }
            fs.readFile(path, "utf8", (err, data) => {
                if (err) {
                    return reject(err);
                }
                resolve(JSON.parse(data));
            });
        })
    };

    return yargs
        .usage("Usage: $0 [options]")
        .help("h")
        .alias("h", "help")
        .options(options).argv;
}
module.exports = readOptions;
