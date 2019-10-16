const axios = require("axios");

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

const pingMonitoring = (url, serviceName, status) => {
  return axios({
    method: "POST",
    url,
    data: {
      service_type: serviceName,
      service_ip: getIPAddress(),
      status,
    }
  });
};

module.exports.pingMonitoring = pingMonitoring;
