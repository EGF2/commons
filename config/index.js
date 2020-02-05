const _merge = require("lodash/merge");
/*
    This module reads variables from env with the prefix "egf_" and generates a config
    that will return as the result of the function.
    The project root directory is expected to contain the config/config.js file
    In the config.js file the shouldVar(Array) field is expected, which indicates which variables are expected
    if there are no variables specified in env, an exception will be thrown in shouldVar.
    All other fields in config.js will be merged with the config from env, priority for env.
*/

let config = {};

const json2obj = str => {
    if (!str) return false;
    try {
      return JSON.parse(str);
    } catch (e) {
      return str;
    }
};

const initConfig = () => {
    if (Object.keys(config).length) return config;
    let configApp;
    try {
        configApp = require(`${module.filename.split("/node_modules")[0]}/config/config.js`);
    } catch (e) {
        configApp = {};
        console.log("WARNING. Not found file config/config.js")
    }
    if (!process.env) throw new Error("Not found var in env");

    if (configApp.shouldVar) {
        for (const field of configApp.shouldVar) {
            if (process.env[`egf_${field}`]) config[field] = json2obj(process.env[`egf_${field}`]);
        }
        if (Object.keys(config).length !== configApp.shouldVar.length){
            const varsInEnv = Object.keys(config) || [];
            const delta = configApp.shouldVar.filter(varInApp => !varsInEnv.includes(varInApp));
            throw new Error(`Not all variables specified in shouldVar are found in env: ${delta.toString()}`)
        }

        delete configApp.shouldVar;
    } else {
        console.log("WARNING. Variable 'shouldVar' in config is empty")
        return {};
    }


    if (Object.keys(configApp).length) config = _merge(configApp, config);
    return config;
};

module.exports = initConfig();
