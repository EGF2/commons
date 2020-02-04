const AWS = require("aws-sdk");

const getProccesor = (kinesis, config, eventHandler, errorHandler) => async (err, shardIteratordata) => {
    if (err) {
        console.log("Kinesis: Error get shard iteration", err);
        process.exit(1)
    }
    console.log("Kinesis: Get shard iteration successfully");
    const Limit = config.kinesisLimit || null;
    if (!Limit) console.log("WARNING! Kinesis limit not set in config");
    let iteration = shardIteratordata.ShardIterator
    while (iteration) {
        try {
            // eslint-disable-next-line no-loop-func
            iteration = await new Promise((resolve, reject) => {
                kinesis.getRecords({ ShardIterator: iteration, Limit },
                    async (err, recordsData) => {
                        try {
                            if (err) reject(err);
                            if (!recordsData) return resolve(null);
                            const systemMessages = [];

                            // to group the events by type
                            const groups = {};
                            recordsData.Records.forEach(record => {
                                const event = JSON.parse(record.Data.toString('utf-8'));
                                const type = event.current
                                    ? event.current.object_type
                                    : event.edge ? `${event.edge.src}/${event.edge.edgeName}` : "system";

                                if (type === "system") systemMessages.push(event);
                                else groups[type]
                                    ? groups[type].push(event)
                                    : groups[type] = [event];
                            });

                            // processing groups
                            for (const type in groups) {
                                const events = groups[type];
                                await eventHandler(events, type);
                            }

                            systemMessages.forEach(message => console.log("System: ", JSON.stringify(message)));
                            resolve(recordsData.NextShardIterator);
                        } catch (e) {
                            errorHandler(e);
                        }
                    },
                );
            });
        } catch (error) {
            if (!error.retryable) errorHandler(error)
        }
    }
}

module.exports = async (config, eventHandler, errorHandler) => {
    try {
        const kinesis = new AWS.Kinesis({
            region: "us-east-1",
        });

        const stream = await new Promise(async (resolve, reject) => {
            kinesis.describeStream({ StreamName: config.kinesisStream },
                (err, streamData) => {
                    if (err) {
                        console.log("Kinesis: Error describe Stream successfully");
                        reject(err);
                    }
                    console.log("Kinesis: Describe Stream successfully");
                    resolve(streamData);
                },
            );
        });

        stream.StreamDescription.Shards.forEach(shard => {
            kinesis.getShardIterator(
                {
                    ShardId: shard.ShardId,
                    ShardIteratorType: "TRIM_HORIZON",
                    StreamName: config.kinesisStream,
                },
                getProccesor(kinesis, config, eventHandler, errorHandler),
            );
        });
    } catch (error) {
        console.log("Kinesis error: ", error);
        process.exit(1);
    }
};