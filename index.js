import * as dotenv from 'dotenv'
import fetch from 'node-fetch';
import { Headers } from 'node-fetch';
import fs from 'fs'
import db from './utils/db.js';

/* THUNDERHEAD DB CONTAINS THE BASIC DATA FROM COMPANIES HOUSE FREE DATA DUMP: https://download.companieshouse.gov.uk/en_output.html 
THIS APP STARTS AT getAccounts() ln 256. 

FIRST getAccounts() CALLS const item getCompanies() THAT RETURNS AN ARRAY OF COMPANIES DEFINED IN 'QUERY'.

THERE IS NO POINT GETTING COMPANIES THAT DONT HAVE 'FULL' ACCOUNTS SO AT A MINIMUM THE QQERY SHOULD
INCLUDE 'WHERE accounts_category = FULL.

THEN fetchFilings() IS CALLED. THIS LOOPS THROUGH THE ARRAY OF COMPANIES AND GRABS THE COMPANY NUMBER.
WITH THAT IT CALLS THE API AND RETURNS THE URL OF THE FILING HISTORY OBJECTS FOR THAT COMPANY.

WE THEN FETCH THE FILING HISTORY OBJECT. THERE ARE LOTS OF ITEMS IN HERE, WE ONLY WANT THE ONES WITH FULL ACCOUNTS.
THESE HAVE ACCOUNT TYPE OF 'AA' AND DESCRIPTION OF "accounts-with-accounts-type-full".

WE ALSO DO SOME CLEVER STUFF TO AVOID TRIGGERING THE API RATE LIMIT. THE APP WILL WAIT 5 MINS IF IT HITS THE LIMIT.

ONCE WE HAVE THE FILING HISTORY OBJECTS WE LOOP THROUGH THEM AND GRAB THE LINKS TO THE PDF'S. WE ALSO GRAB THE DATE

THE FILING HISTORY OF EACH COMPANY IN THE DB.

*/
// to run use: node index.js n from the terminal, where n = the batch number (look in the Accounts
// folder to get the current batch. 

// 1. TODO: Add the current batch number to the database to track it)

// 2. TODO: Update the database with the new accounts object and the link to the pdf's (line 196)

// 3. TODO: Add this data to the Next.js frontend

dotenv.config()

// global api key
var key = process.env.KEY
// index of api call
let index = process.argv[2]

async function getCompanies(data) {

  let items = data;

  for await (var item of items) {

    let account_filing = await fetchFiling(item);

    //console.log('account_filing: ' + JSON.stringify(account_filing) + '\n')
    if (account_filing.filing_history_status === "filing-history-available") {
      item.account_filing = account_filing;
    }
  }

  return items
}

async function getAccounts(company) {

  let item = company;
  console.log('item: ' + JSON.stringify(item.account_filing.items) + '\n')
  let count = 0;
  // loop through each company in items and add the filing history object.

  for await (var filing of item.account_filing.items) {

    // CHECK IF THE FILING IS AN ACCOUNTS FILING AND IF IT IS FULL ACCOUNTS
    // AND WE ONLY WANT THE LAST 2 ACCOUNT FILINGS SO WE USE THE account_no VARIABLE 
    // account_no WILL BE 1 FOR THE FIRST ACCOUNT FILING AND 2 FOR THE SECOND

    if (filing.description === "accounts-with-accounts-type-full" && count < 2) {
      console.log('count: ' + count)
      console.log(`Company number: ${item.number} \nCompany name: ${item.name} \n`)

      let headers = new Headers();

      //headers.append('Content-Type', 'text/xml')
      headers.append('Accept', 'application/pdf')
      headers.append('Authorization', 'Basic ' + btoa(key))

      //header object
      var obj = {
        method: 'GET',
        headers: headers
      }

      //2. fetch the document metadata

      try {

        const metadata = await (await fetch(filing.links.document_metadata, obj)).json()
        let document = metadata.links.document

        // console.log('metadata: ' + JSON.stringify(metadata))

        const doc = await fetch(document, obj)

        // console.log(doc.url)

        var dir = `./Accounts/`;
        var link = `${item.name.replace(/[\s\\\/]/g, "_")}_${count}.pdf`

        if (!fs.existsSync(dir)) {
          fs.mkdirSync(dir);
        }



        const doc_link = `${dir}${link}`
        await downloadFile(doc.url, doc_link)
        await insertData(link, item.id, count)
        count++

      } catch (error) {
        console.error(error)
      }
    }
  }
}

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
    console.log("done!")
  });
});

async function insertData(link, id, number) {

  let x;

  if (number === 1) {
    x = 'accounts_link_1'
  } else {
    x = 'accounts_link_2'
  }

  try {
    const query = `UPDATE companies SET "${x}" = $1 WHERE id = $2`;
    const values = [link, id];

    const res = await db.query(query, values);
    console.log('Data inserted successfully' + JSON.stringify(res));
  } catch (err) {
    console.error('Error inserting data: ', err);
  }
}

async function getBatches(batchSize) {
  // getCompanies returns an array of active companies with a specific sic code

  const countQuery = `SELECT COUNT(*) FROM companies WHERE accounts_category = 'FULL' AND status = 'Active'`;
  const countResult = await db.query(countQuery);
  const totalRows = parseInt(countResult.rows[0].count, 10);

  for (let offset = 0; offset < totalRows; offset += batchSize) {
    console.log(`Getting companies ${offset} to ${offset + batchSize} of ${totalRows}...`);
    const selectQuery = `SELECT * FROM companies WHERE accounts_category = 'FULL' AND status = 'Active' LIMIT ${batchSize} OFFSET ${offset}`;
    const selectResult = await db.query(selectQuery);
    const data = selectResult.rows;

    //console.log('data: ' + JSON.stringify(data) + '\n')

    let companies = await getCompanies(data);

    for await (var company of companies) {
      let accounts = await getAccounts(company);
      console.log(accounts)
    }
  }
}

getBatches(300)