const Kafka = require("node-rdkafka");

class ProduserClient {
  constructor(config) {
    this.config = {
      hosts: config.kafka.hosts,
      topic: config.kafka.systemErrorTopic
    };
  }

  createProducer() {
    this.producer = new Kafka.HighLevelProducer({
      "metadata.broker.list": this.config.hosts,
      "api.version.request.timeout.ms": 1000
    });
    this.producer.connect();
  }

  _sendMessage(message, partition) {
    // In case does't systemErrorTopic
    if (!this.config.topic) return null;

    return new Promise((resolve, reject) => {
      try {
        this.producer.produce(
          this.config.topic,
          partition,
          Buffer.from(message),
          null,
          Date.now(),
          (err, offset) => {
            if (err) reject(err);
            console.log("Send message to kafka", {
              topic: this.config.topic,
              partition,
              offset,
              message
            });
            resolve(offset);
          }
        );
      } catch (error) {
        reject(error);
      }
    });
  }
  async sendEvent(event) {
    let action = "Send event to Kafka";
    try {
      await this._sendMessage(JSON.stringify(event), 0);
    } catch (e) {
      console.log("ERROR SEND TO system_error queue");
      console.log(`${action} failed.`, e, { event });
      process.exit(1);
    }
  }
}

module.exports = ProduserClient;
