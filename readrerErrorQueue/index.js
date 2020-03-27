const csv = require("csvtojson");

class ReaderErrorQueue {
  constructor(eventHandler) {}

  async _getContent(path_to_content) {
    let content = [];
    try {
      content = await csv().fromFile(path_to_content);
    } catch (err) {
      console.error("ContentError", err);
    }
    return content;
  }



  readFile(pathToFile) {}
}
