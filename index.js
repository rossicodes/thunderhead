// imports
import * as dotenv from 'dotenv'
import fetch from 'node-fetch';
import { Headers } from 'node-fetch';
import fs from 'fs'
import db from './utils/db.js';
import { S3Client } from "@aws-sdk/client-s3";
import { PutObjectCommand } from "@aws-sdk/client-s3";

const REGION = 'eu-north-1';
const s3Client = new S3Client({ region: REGION });

// setup env variables
dotenv.config()

// global api key
var key = process.env.KEY

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// script starts here, gets companies from db in batches specified when calling e.g. getBatches(300)
async function getBatches(batchSize) {
  // countQuery returns the total number of companies returned by the query
  const sic = '63990 - Other information service activities n.e.c.'

  // '47910 - Retail sale via mail order houses or via Internet'
  // '63120 - Web portals'
  // '63990 - Other information service activities n.e.c.', sic_code_2: '63990 - Other information service activities n.e.c.'

  const countQuery = {
    text: "SELECT COUNT(*) FROM ecommerce",
    values: [],
  }

  const countResult = await db.query(countQuery);
  // console.log('countResult: ' + JSON.stringify(countResult) + '\n')
  const totalRows = parseInt(countResult.rows[0].count, 10);

  for (let offset = 0; offset < totalRows; offset += batchSize) {
    console.log(`Getting companies ${offset} to ${offset + batchSize} of ${totalRows}...`);

    const query = {
      text: "SELECT * FROM ecommerce LIMIT $1 OFFSET $2",
      values: [batchSize, offset],
    }
    const selectResult = await db.query(query);
    const data = selectResult.rows;

    // getCompanies returns an array of companies defined in selctQuery
    let companies = await getCompanies(data);
    // for each row in the array of companies, getAccounts returns 
    let companyCount = companies.length;
    console.log(`Analysing ${companyCount} companies...`);
    for await (var company of companies) {
      await sleep(1000);
      await getAccounts(company);
      // console.log(`${company.name}...\n`)
    }
  }
}
// returns an array of companies - called from getBatches
async function getCompanies(data) {

  let items = data;
  console.log("Getting filing history")

  for await (var item of items) {

    let account_filing = await fetchFiling(item);
    await sleep(1000);
    //console.log('account_filing: ' + JSON.stringify(account_filing) + '\n')
    if (account_filing?.filing_history_status === "filing-history-available") {
      // console.log(`Filing history available for ${item.name}`)
      item.account_filing = account_filing;

    }
  }

  return items
}
// calls fetchFiling and adds to the company object then downloads the pdf
// then adds the link to the database in the corrosponding row
async function getAccounts(company) {

  let item = company;
  // console.log('item: ' + JSON.stringify(item.account_filing.items) + '\n')
  let count = 0;

  // loop through each company object in items and add the relevant filing history object

  // console.log(`Getting Accounts for:\nCompany name: ${JSON.stringify(item)}`)

  if (item?.account_filing?.items !== undefined) {

    for await (var filing of item?.account_filing.items) {


      console.log('filing : ' + JSON.stringify(filing) + '\n')

      // CHECK IF THE FILING IS AN ACCOUNTS FILING AND IF IT IS FULL ACCOUNTS
      // AND WE ONLY WANT THE LAST 2 ACCOUNT FILINGS SO WE USE THE account_no VARIABLE 
      // account_no WILL BE 1 FOR THE FIRST ACCOUNT FILING AND 2 FOR THE SECOND

      if (count < 2) {

        if (filing.description ? === "accounts-with-accounts-type-full" || "accounts-with-accounts-type-full-group" || "accounts-with-accounts-type-initial" || "accounts-with-accounts-type-interim" || "accounts-with-accounts-type-group" || "accounts-with-accounts-type-medium" || "accounts-with-accounts-type-medium-group" || "accounts-with-accounts-type-small" || "accounts-with-accounts-type-small-group") {
          await sleep(1000);
          //console.log('count: ' + count)
          console.log(`Getting ${filing.description} accounts for: ${item.name}`)

          let headers = new Headers();

          //headers.append('Content-Type', 'text/xml')
          headers.append('Accept', 'application/pdf')
          headers.append('Authorization', 'Basic ' + btoa(key))

          //header object
          var obj = {
            method: 'GET',
            headers: headers
          }

          //fetch the document metadata

          try {

            const metadata = await (await fetch(filing.links.document_metadata, obj)).json()

            if (metadata?.category === "accounts") {
              console.log('metadata: ' + JSON.stringify(metadata) + '\n')

              let document = metadata.links.document

              const doc = await fetch(document, obj)

              // console.log(doc.url)

              var dir = `./Accounts/`;
              var link = `${item.number.replace(/[\s\\\/]/g, "_")}_${count}.pdf`
              // make the directory if it doesn't exist
              if (!fs.existsSync(dir)) {
                fs.mkdirSync(dir);
              }
              // link to the file
              const doc_link = `${dir}${link}`
              // download the file and
              await downloadFile(doc.url, doc_link)
              // upload the file to s3
              const s3url = await uploadS3(dir, link)
              console.log('uploaded to s3')
              // and insert the data into the database
              await insertData(s3url, item.id, count)
              console.log('added to db')
              // increment the count so we only get the last 2 account pdf's
              count++
            }

          } catch (error) {
            console.error(error)
          }

        }
      }
    }
  } else {
    console.log(`No filing history found for ${item.name}`)
  }

}
// called from getAccounts to get the filing history for each company
async function fetchFiling(item) {

  // limit number of calls to 500 every 5 mins

  // loop through the array of objects and grab the filing history URL.

  let company_number = item.number;

  var url = `https://api.companieshouse.gov.uk/company/${company_number}/filing-history`

  //console.log('FILILING HISTORY URL: ' + url + '\n')

  let headers = new Headers();

  //headers.append('Content-Type', 'text/xml')
  headers.append('Accept', 'application/pdf')
  headers.append('Authorization', 'Basic ' + btoa(key))

  //header object
  var obj = {
    method: 'GET',
    headers: headers
  }
  // get company filing history

  try {
    const response = await fetch(url, obj);
    const data = await response.json();
    // console.log('FILING HOSTORY OBJECT: ' + JSON.stringify(data) + '\n')

    const xRateLimitRemain = response.headers.get('x-ratelimit-remain')
    const xRateLimitReset = response.headers.get('x-ratlimit-reset');
    // console.log("xRateLimitRemain: " + xRateLimitRemain + " " + typeof xRateLimitRemain)

    if (xRateLimitRemain === '10') {
      console.log('Hit the API rate limit. Waiting 5 mins...');
      await new Promise(resolve => setTimeout(resolve, 360000));
      console.log('Resuming...');
    }

    return data;

  } catch (error) {
    console.log('Error happened here!')
    console.error(error)
  }
}
//function to download the .pdf file given the Amazon S3 url 
const downloadFile = (async (url, path) => {
  const res = await fetch(url);
  const fileStream = fs.createWriteStream(path);
  await new Promise((resolve, reject) => {
    res.body.pipe(fileStream);
    res.body.on("error", reject);
    fileStream.on("finish", resolve);
    // console.log("Accounts Downloaded")
  });
});
// called from getAccounts to insert the link to the pdf into the database
async function insertData(s3url, id, number) {

  let x;

  if (number === 0) {
    x = 'accounts_link_1'
  } else {
    x = 'accounts_link_2'
  }

  try {
    const query = {
      text: `UPDATE ecommerce SET ${x} = $1 WHERE id = $2`,
      values: [s3url, id],
    }
    console.log("Query: " + JSON.stringify(query));

    const res = await db.query(query);
    console.log("From db: " + res);
  } catch (err) {
    console.error('Error inserting data: ', err);
  }
}

async function uploadS3(dir, link) {

  const bucketUrl = 'https://uk-company-accounts.s3.eu-north-1.amazonaws.com/'

  const fileName = `${dir}${link}`;
  const fileBuffer = fs.readFileSync(fileName);
  const params = {
    Bucket: 'uk-company-accounts',
    Key: link,
    Body: fileBuffer,
    ContentType: 'application/pdf'
  };

  try {
    const data = await s3Client.send(new PutObjectCommand(params));
    //  console.log("uploaded to S3");
  } catch (err) {
    console.log("Error", err);
  }
  // console.log("S3 Url: " + bucketUrl + params.Key)
  return bucketUrl + params.Key;
}

// start the process
getBatches(50)