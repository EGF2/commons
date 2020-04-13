const { argv } = require('yargs');
const csv = require("csvtojson");
const lineByLine = require('n-readlines');
const liner = new lineByLine(argv.file);

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
    let line = liner.next();
    let firstString;
    let lineNumber = 0;
    while (line) {
      if (lineNumber === 0) {
        firstString = line;
        line = liner.next();
      }
      const result = await getFileContent(firstString, line);
      const event = JSON.parse(result[0].event);
      await eventHandler(event);
      lineNumber++;
      line = liner.next();
    }
  } catch (e) {
    errorHandler(e);
  }
};

module.exports = newConsumer;