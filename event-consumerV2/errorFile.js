const { argv } = require('yargs');
const fs = require('fs');
const readline = require('readline');
const csv = require("csvtojson");
const getFileContent = async (first, line) => {
  let result = {};
  try {
    const csvString = `${first}\n${line}`;
    result = await csv({
      noheader: false,
      output: "json",
      ignoreEmpty: true
    }).fromString(csvString);
  } catch (err) {
    throw new Error(err.message);
  }
  return result;
};

const newConsumer = async (config, eventHandler, errorHandler) => {
  try {
    let firstString = null;
    const read = readline.createInterface({
      input: fs.createReadStream(argv.file),
      output: process.stdout,
      console: false
    });
    read.on('line', async line => {
      if (line) {
        if (!firstString) {
          firstString = line;
          return;
        }
        const result = await getFileContent(firstString, line);
        await eventHandler(result[0].event);
      } else {
        console.log("END")
      }
    });
  } catch (e) {
    errorHandler(e);
  }
};

module.exports = newConsumer;