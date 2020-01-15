const _merge = require("lodash/merge");

let config = {};

/*
    This module reads variables from env with the prefix "egf_" and generates a config
    that will return as the result of the function.
    The project root directory is expected to contain the config/config.js file
    In the config.js file the shouldVar(Array) field is expected, which indicates which variables are expected
    if there are no variables specified in env, an exception will be thrown in shouldVar.
    All other fields in config.js will be merged with the config from env, priority for env.
*/

const initConfig = () => {
    if (Object.keys(config).length) return config;
    let configApp;
    try {
        configApp = require("./config/config.js");
    } catch (e) {
        configApp = {};
        console.log("WARNING. Not found file config/config.js")
    }
    if (!process.env) throw new Error("Not found var in env");

    if(configApp.shouldVar) {
        for(const field of configApp.shouldVar) {
            if (process.env[`egf_${field}`]) config[field] = process.env[`egf_${field}`];
        }
        if(Object.keys(config).length !== configApp.shouldVar.length)
            throw new Error("Not all variables specified in shouldVar are found in env")

        delete configApp.shouldVar;
    } else console.log("WARNING. Variable 'shouldVar' in config is empty")


    if (Object.keys(configApp).length) config = _merge(configApp, config);
    return config;
};

module.exports = initConfig();
