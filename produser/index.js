const Kafka = require("node-rdkafka");

class Producer {
  constructor(config) {
    this.config = {
      hosts: config.kafka.hosts,
      topic: config.kafka.systemErrorTopic
    };
  }

  createProducer() {
    const producer = new Kafka.HighLevelProducer({
      "metadata.broker.list": this.config.hosts,
      "api.version.request.timeout.ms": 1000
    });
    producer.connect();
  }

  _sendMessage(message, partition) {
    // In case does't systemErrorTopic
    if (!this.config.topic) return null;

    return new Promise((resolve, reject) => {
      try {
        producer.produce(
          config.topic,
          partition,
          Buffer.from(message),
          null,
          Date.now(),
          (err, offset) => {
            if (err) reject(err);
            Log.info("Send message to kafka", {
              topic: config.topic,
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
      await sendMessage(JSON.stringify(event), 0);
    } catch (e) {
      console.log("ERROR SEND TO system_error queue");
      Log.error(`${action} failed.`, e, { event });
      process.exit(1);
    }
  }
}

module.exports = Producer;
