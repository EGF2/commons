const AWS = require("aws-sdk");

const getProccesor = (kinesis, eventHandler, errorHandler) => async (err, shardIteratordata) => {
    if (err) {
        console.log("Kinesis: Error get shard iteration", err);
        process.exit(1)
    }
    console.log("Kinesis: Get shard iteration successfully");
    let iteration = shardIteratordata.ShardIterator
    while (iteration) {
        try {
            // eslint-disable-next-line no-loop-func
            iteration = await new Promise((resolve, reject) => {
                kinesis.getRecords({ ShardIterator: iteration },
                    async (err, recordsData) => {
                        try {
                            if (err) reject(err);
                            if (!recordsData) return resolve(null);
                            await eventHandler(recordsData.Records.map(record => JSON.parse(record.Data.toString('utf-8'))));
                            resolve(recordsData.NextShardIterator);
                        } catch (e) {
                            errorHandler(e);
                        }
                    },
                );
            });
        } catch (error) {
            if (!err.retryable) errorHandler(error)
        }
    }
}

const newConsumer = async (config, eventHandler, errorHandler) => {
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
                getProccesor(kinesis, eventHandler, errorHandler),
            );
        });
    } catch (error) {
        console.log("Kinesis error: ", error);
        process.exit(1);
    }
}

module.exports = newConsumer;

