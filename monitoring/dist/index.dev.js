"use strict";

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

var axios = require("axios");

var MonitoringClient =
/*#__PURE__*/
function () {
  /**@config {Object} {monitoring: <url monitoring>, serviceName: <service name>} */
  function MonitoringClient(config) {
    _classCallCheck(this, MonitoringClient);

    this._url = config.monitoring;
    this._serviceName = config.serviceName;
  }

  _createClass(MonitoringClient, [{
    key: "_getIPAddress",
    value: function _getIPAddress() {
      var interfaces = require("os").networkInterfaces();

      for (var devName in interfaces) {
        var iface = interfaces[devName];

        for (var i = 0; i < iface.length; i++) {
          var alias = iface[i];

          if (alias.family === "IPv4" && alias.address !== "127.0.0.1" && !alias.internal) {
            return alias.address;
          }
        }
      }

      return "0.0.0.0";
    }
  }, {
    key: "_getSalt",
    value: function _getSalt() {
      var result;
      return regeneratorRuntime.async(function _getSalt$(_context) {
        while (1) {
          switch (_context.prev = _context.next) {
            case 0:
              _context.next = 2;
              return regeneratorRuntime.awrap(axios.get("".concat(process.env.ECS_CONTAINER_METADATA_URI, "/task")));

            case 2:
              result = _context.sent;
              salt = "".concat(result.data.Family, "|").concat(result.data.Containers[0].Name, "|").concat(result.data.TaskARN.split("/")[1]);
              return _context.abrupt("return", salt);

            case 5:
            case "end":
              return _context.stop();
          }
        }
      });
    }
    /**@status {String} UP or DOWN */

  }, {
    key: "pingMonitoring",
    value: function pingMonitoring(status) {
      var res;
      return regeneratorRuntime.async(function pingMonitoring$(_context2) {
        while (1) {
          switch (_context2.prev = _context2.next) {
            case 0:
              if (this._salt) {
                _context2.next = 4;
                break;
              }

              _context2.next = 3;
              return regeneratorRuntime.awrap(this._getSalt());

            case 3:
              this._salt = _context2.sent;

            case 4:
              _context2.next = 6;
              return regeneratorRuntime.awrap(axios({
                method: "POST",
                url: "".concat(this._url, "/v2/internal/monitoring/status"),
                data: {
                  service_type: this._serviceName,
                  service_ip: this._getIPAddress(),
                  status: status,
                  salt: salt
                }
              }));

            case 6:
              res = _context2.sent;
              return _context2.abrupt("return", res);

            case 8:
            case "end":
              return _context2.stop();
          }
        }
      }, null, this);
    }
    /**@eventId {String} */

  }, {
    key: "sendFailEvent",
    value: function sendFailEvent(eventId) {
      return regeneratorRuntime.async(function sendFailEvent$(_context3) {
        while (1) {
          switch (_context3.prev = _context3.next) {
            case 0:
              _context3.next = 2;
              return regeneratorRuntime.awrap(axios({
                method: "POST",
                url: "".concat(this._url, "/v2/internal/monitoring/send_error"),
                data: {
                  service: this._serviceName,
                  event: eventId
                }
              }));

            case 2:
            case "end":
              return _context3.stop();
          }
        }
      }, null, this);
    }
  }]);

  return MonitoringClient;
}();

module.exports.MonitoringClient = MonitoringClient;
var salt = "";

var getIPAddress = function getIPAddress() {
  var interfaces = require("os").networkInterfaces();

  for (var devName in interfaces) {
    var iface = interfaces[devName];

    for (var i = 0; i < iface.length; i++) {
      var alias = iface[i];

      if (alias.family === "IPv4" && alias.address !== "127.0.0.1" && !alias.internal) {
        return alias.address;
      }
    }
  }

  return "0.0.0.0";
};

var pingMonitoring = function pingMonitoring(url, serviceName, status) {
  return new Promise(function _callee(resolve, reject) {
    var result, res;
    return regeneratorRuntime.async(function _callee$(_context4) {
      while (1) {
        switch (_context4.prev = _context4.next) {
          case 0:
            _context4.prev = 0;

            if (salt) {
              _context4.next = 12;
              break;
            }

            _context4.prev = 2;
            _context4.next = 5;
            return regeneratorRuntime.awrap(axios.get("".concat(process.env.ECS_CONTAINER_METADATA_URI, "/task")));

          case 5:
            result = _context4.sent;
            salt = "".concat(result.data.Family, "|").concat(result.data.Containers[0].Name, "|").concat(result.data.TaskARN.split("/")[1]);
            _context4.next = 12;
            break;

          case 9:
            _context4.prev = 9;
            _context4.t0 = _context4["catch"](2);
            return _context4.abrupt("return", resolve());

          case 12:
            _context4.next = 14;
            return regeneratorRuntime.awrap(axios({
              method: "POST",
              url: url,
              data: {
                service_type: serviceName,
                service_ip: getIPAddress(),
                status: status,
                salt: salt
              }
            }));

          case 14:
            res = _context4.sent;
            resolve(res);
            _context4.next = 21;
            break;

          case 18:
            _context4.prev = 18;
            _context4.t1 = _context4["catch"](0);
            reject(_context4.t1);

          case 21:
          case "end":
            return _context4.stop();
        }
      }
    }, null, null, [[0, 18], [2, 9]]);
  });
};

module.exports.pingMonitoring = pingMonitoring;