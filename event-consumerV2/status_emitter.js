const axios = require("axios");
const os = require("os");

const Logging = require("../Logging");

const ifaces = os.networkInterfaces();
const Log = new Logging(__filename);

const getId = () => {
    const addreses = [];
    Object.keys(ifaces).forEach(function(ifname) {
        ifaces[ifname].forEach(function(iface) {
            if (iface.family !== "IPv4" || iface.internal !== false) return;
            addreses.push(iface.address);
        });
    });
    return addreses.join(", ");
};

class StatusEmitter {
    constructor(config) {
        this.config = config;
    }

    async sendStatus(status) {
        const message = {
            service_type: this.config.serviceName,
            service_ip: getId(),
            status,
        };

        try {
            const url = `${this.config.egf_api}/v2/monitoring/status`;
            await axios.post(url, message);
        } catch (e) {
            Log.error("Status emitter error", e, {}, true);
        }
    }
}

module.exports = StatusEmitter;
