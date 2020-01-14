let config = {};

const initConfig = () => {
    console.log(__dirname)
    if (Object.keys(config).length) return config;
    let configApp = require("./config/config.js");
    if (!process.env) throw new Error("Not found var in env");

    for(const field of configApp.shouldVar) {
        if (process.env[`egf_${field}`]) config[field] = process.env[`egf_${field}`];
    }
    delete configApp.shouldVar;

    if (Object.keys(configApp).length) {
        config = Object.assign(configApp, config);
    }
    return config;
}


module.exports = initConfig();
