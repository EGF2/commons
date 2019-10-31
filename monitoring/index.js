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

const pingMonitoring = (url, serviceName, status, port) => {
  return new Promise(async (resolve, reject) => {
    const resonce = await axios.get({
      method: "GET",
      url: `http://localhost:${process.env.egf_port || port}/v2/metadata`
    });
    console.log(resonce);
    try {
      const res = await axios({
        method: "POST",
        url,
        data: {
          service_type: serviceName,
          service_ip: getIPAddress(),
          status,
        }
      });
      resolve(res)
    } catch (e) {
      reject(e)
    }
  });
};

module.exports.pingMonitoring = pingMonitoring;
