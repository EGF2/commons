const axios = require("axios");

class MonitoringClient {
  /**@config {Object} {monitoring: <url monitoring>, serviceName: <service name>} */
  constructor(config) {
    this._url = config.monitoring;
    this._serviceName = config.serviceName;
  }

  _getIPAddress = () => {
    const interfaces = require("os").networkInterfaces();
    for (const devName in interfaces) {
      const iface = interfaces[devName];
      for (let i = 0; i < iface.length; i++) {
        const alias = iface[i];
        if (
          alias.family === "IPv4" &&
          alias.address !== "127.0.0.1" &&
          !alias.internal
        ) {
          return alias.address;
        }
      }
    }
    return "0.0.0.0";
  };

  async _getSalt() {
    const result = await axios.get(
      `${process.env.ECS_CONTAINER_METADATA_URI}/task`
    );
    salt = `${result.data.Family}|${result.data.Containers[0].Name}|${
      result.data.TaskARN.split("/")[1]
    }`;
    return salt;
  }

  /**@status {String} UP or DOWN */
  async pingMonitoring(status) {
    if (!this._salt) {
      this._salt = await this._getSalt();
    }
    const res = await axios({
      method: "POST",
      url: `${this._url}/v2/internal/monitoring/status`,
      data: {
        service_type: this._serviceName,
        service_ip: this._getIPAddress(),
        status,
        salt
      }
    });
    return res;
  }

  /**@eventId {String} */
  async sendFailEvent(eventId) {
    await axios({
      method: "POST",
      url: `${this._url}/v2/internal/monitoring/event_fail`,
      data: {
        service_name: this._serviceName,
        event_id: eventId
      }
    });
  }
}

module.exports.MonitoringClient = MonitoringClient;

let salt = "";

const getIPAddress = () => {
  const interfaces = require("os").networkInterfaces();
  for (const devName in interfaces) {
    const iface = interfaces[devName];
    for (let i = 0; i < iface.length; i++) {
      const alias = iface[i];
      if (
        alias.family === "IPv4" &&
        alias.address !== "127.0.0.1" &&
        !alias.internal
      ) {
        return alias.address;
      }
    }
  }
  return "0.0.0.0";
};

const pingMonitoring = (url, serviceName, status) =>
  new Promise(async (resolve, reject) => {
    try {
      if (!salt) {
        try {
          const result = await axios.get(
            `${process.env.ECS_CONTAINER_METADATA_URI}/task`
          );
          salt = `${result.data.Family}|${result.data.Containers[0].Name}|${
            result.data.TaskARN.split("/")[1]
          }`;
        } catch (e) {
          return resolve();
        }
      }

      const res = await axios({
        method: "POST",
        url,
        data: {
          service_type: serviceName,
          service_ip: getIPAddress(),
          status,
          salt
        }
      });
      resolve(res);
    } catch (e) {
      reject(e);
    }
  });

module.exports.pingMonitoring = pingMonitoring;
