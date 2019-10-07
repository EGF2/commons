const config = require("config");
const Sentry = require("@sentry/node");

Sentry.init({
    dsn: "https://3ae99f681a2045b7ba3f4c1fde633f8f@sentry.io/1417634",
    defaultIntegrations: false,
    debug: config.sentryDebug || false,
    environment: process.env.NODE_ENV,
    serverName: config.serviceName,
    beforeSend(event) {
        if (!["dev", "stage", "prod"].includes(process.env.NODE_ENV)) {
            return null;
        }
        if (event.message) {
            return event.message.match(/SentryError: HTTP Error \(429\)/) ? null : event;
        }
        return event;
    }
});

const intLevel = {
    debug: 1,
    info: 2,
    warning: 3,
    error: 4
};

class Logging {
    constructor(tag) {
        this.tag = tag;
        this.configSendtoSentry = config.sendtoSentry;
        this.logLevel = intLevel[config.log_level];
    }

    write(data, e, sendtoSentry) {
        if (intLevel[data.level.toLowerCase()] >= this.logLevel) {
            console.log(`${JSON.stringify(data)}\n`);
        }

        if (this.configSendtoSentry && sendtoSentry) {
            const user = data.params && data.params.user;
            Sentry.configureScope(async scope => {
                scope.setTag("service", config.serviceName);
                scope.setUser({id: user});
                scope.setLevel(data.level);
                Sentry.addBreadcrumb({message: data.message});
                Sentry.captureException(e || new Error());
            });
        }
    }

    generateLog(level, text, params, e) {
        const data = {
            level,
            location: this.tag,
            message: `${text} ${e ? e.message : ""}`
        };
        if (params) {
            data.params = params;
        }
        if (e) {
            data.eStack = e.stack;
        }
        return data;
    }

    expandTag(tag) {
        const splitTag = this.tag.split("|");
        this.tag = `${splitTag[0]}|${tag}`;
    }

  /** @text text message, @params params warming, @sendToSentry true - send, false - do not send */
    info(text, params, sendToSentry) {
        const data = this.generateLog("INFO", text, params);
        this.write(data, "", sendToSentry);
    }

  /** @text text message, @params params warming, @sendToSentry true - send, false - do not send */
    debug(text, params, sendToSentry) {
        const data = this.generateLog("DEBUG", text, params);
        this.write(data, "", sendToSentry);
    }

  /** @text text message, @e object error, @params params error @sendToSentry  true - send, false - do not send */
    error(text, e, params, sendToSentry) {
        if (typeof text !== "string") {
            params = e;
            e = text;
            text = "";
        }
        const data = this.generateLog("ERROR", text, params, e);
        this.write(data, e, sendToSentry);
    }

  /** @text text message, @params params warming, @sendToSentry true - send, false - do not send */
    warning(text, params, sendToSentry) {
        const data = this.generateLog("WARNING", text, params);
        this.write(data, "", sendToSentry);
    }
}

module.exports = Logging;
