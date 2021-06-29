require("dotenv").config();
const { Storage } = require("@google-cloud/storage");
const path = require("path");
const serviceKey = path.join(__dirname, process.env.GCS_KEYFILE);
const bucketName = process.env.GCS_BUCKET_NAME;
const fs = require("fs").promises;
const PromisePool = require("@supercharge/promise-pool");

const storage = new Storage({
  keyFilename: serviceKey,
  projectId: process.env.GCS_PROJECT_ID,
});

async function listFilesName(company, country, category, fromDate, endDate) {
  let date = fromDate;
  let filenames = [];
  const [files] = await storage
    .bucket(bucketName)
    .getFiles({ prefix: `${company}/${country}/${category}` });
  while (date != endDate) {
    const regex = new RegExp(
      company + "." + country + "." + category + "." + date
    );
    files.forEach((file) => {
      if (regex.test(file.name)) {
        filenames.push(file.name);
      }
    });
    date = dateIncrement(date);
  }

  return filenames;
}

function dateIncrement(date) {
  var d = date.match(/\d*/);
  var m = date.match(/-\d*/);
  m = m[0].replace(/-/, "");
  var y = date.match(/\d\d\d\d/);
  var mmddyyyy = m + "-" + d + "-" + y;

  let fromdate = new Date(mmddyyyy);

  fromdate.setDate(fromdate.getDate() + 1);

  var day = fromdate.getDate();
  var month = fromdate.getMonth() + 1;
  var year = fromdate.getFullYear();
  if (month.toString().length === 1) {
    month = "0" + month;
  }
  if (day.toString().length === 1) {
    day = "0" + day;
  }
  var ddmmyyyy = day + "-" + month + "-" + year;
  return ddmmyyyy;
}

const fetchFileContents = async (fileName) => {
  return new Promise((resolve, reject) => {
    const readStream = storage
      .bucket(bucketName)
      .file(fileName)
      .createReadStream();
    let buf = "";
    readStream
      .on("data", (d) => {
        buf += d;
      })
      .on("end", () => {
        resolve(buf);
      })
      .on("error", () => {
        reject("error");
      });
  });
};

async function main(company, country, category, fromDate, endDate) {
  const fileNames = await listFilesName(
    company,
    country,
    category,
    fromDate,
    endDate
  ).catch(console.error);
  await fs.appendFile("parser.json", "[");
  let c = 1;
  const { results, errors } = await PromisePool.withConcurrency(100)
    .for(fileNames)
    .process(async (filename) => {
      console.log("fetching", filename);
      let content = await fetchFileContents(filename);

      if (c < fileNames.length) {
        await fs.appendFile("parser.json", JSON.stringify(content) + ",");
      } else if (c === fileNames.length) {
        await fs.appendFile("parser.json", JSON.stringify(content));
      }
      c++;

      return content;
    });

  await fs.appendFile("parser.json", "]");
}

main("zomato.com", "india", "order-delivered", "01-01-2018", "21-11-2019");

// [END storage_list_files]
