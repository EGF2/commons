const axios = require("axios");

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

const sendErrorMessage = (url, service, message) =>
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
          service_type: service,
          service_ip: getIPAddress(),
          message,
          salt
        }
      });
      resolve(res);
    } catch (e) {
      reject(e);
    }
  });

module.exports.pingMonitoring = pingMonitoring;
module.exports.sendErrorMessage = sendErrorMessage;
