import * as dotenv from 'dotenv'
import fetch from 'node-fetch';
import { Headers } from 'node-fetch';
import fs from 'fs'
import db from './utils/db.js'

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

async function getCompanies() {
  // getCompanies returns an array of active companies with a specific sic code

  const query = `
  SELECT *
  FROM companies
  WHERE accounts_category = 'FULL'
  AND status = 'Active'
  AND (sic_code_1 = '63120 - Web portals' OR sic_code_2 = '63120 - Web portals' OR sic_code_3 = '63120 - Web portals' OR sic_code_4 = '63120 - Web portals')
  `;

  const dbquery = await db.query(query)
  const data = await dbquery.rows
  //console.log(data)


  return data;


}



async function getAccounts() {
  // get an array of actve companies with a specific sic code form CH api

  const items = await getCompanies();
  console.log("companies: " + JSON.stringify(items))

  //note the company ID / number in the url. This was just a test, 
  //the production app will need to loop through a list of company id's.
  //perhaps via a search or data dump.

  // filing history url
  // var url = 'https://api.companieshouse.gov.uk/company/12763330/filing-history'
  console.log(`Getting the filing history of ${items.length} companies...`)

  let filings = []
  let accounts = []

  async function fetchFiling(item) {

    // limit number of calls to 500 every 5 mins

    // loop through the array of objects and grab the filing history of each one.

    let company_number = item.number;

    var url = `https://api.companieshouse.gov.uk/company/${company_number}/filing-history`

    console.log(url)

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
      //console.log(data)

      const xRateLimitRemain = response.headers.get('x-ratelimit-remain')
      const xRateLimitReset = response.headers.get('x-ratlimit-reset');
      // console.log("xRateLimitRemain: " + xRateLimitRemain + " " + typeof xRateLimitRemain)

      if (xRateLimitRemain === '10') {
        console.log('Hit the API rate limit. Waiting 5 mins...');
        await new Promise(resolve => setTimeout(resolve, 360000));
        console.log('Resuming...');
      }

      data.items.forEach(function (i) {
        // console.log(i)
        let companyExists = filings.find(obj => obj.number === item.number);
        if (companyExists) {
          if (!companyExists.hasOwnProperty("accountsCount") && item.category == 'accounts') {
            companyExists.accountsCount = 0;
          }
          companyExists.accountsCount++;
          companyExists.accounts["accounts_" + companyExists.accountsCount] = item.accounts;
        } else {

          let companyWithAccounts = {}
          companyWithAccounts.number = item.number
          companyWithAccounts.name = item.name
          companyWithAccounts.accounts = {}
          companyWithAccounts.accounts.links = i.links
          companyWithAccounts.accounts.date = i.date
          companyWithAccounts.accounts.paper_filed = i.paper_filed
          companyWithAccounts.accounts.type = i.type
          companyWithAccounts.accounts.description = i.description
          companyWithAccounts.accounts.pages = i.pages
          companyWithAccounts.accounts.barcode = i.barcode
          companyWithAccounts.accounts.transaction_id = i.transaction_id

          filings.push(companyWithAccounts);
          console.log(`\x1b[32m Analysing ${JSON.stringify(companyWithAccounts.name)}\x1b[0m`)
        }
      })
    } catch (error) {
      console.log('Error happened here!')
      console.error(error)
    }
  }

  for await (var item of items) {
    // console.log(item)
    await fetchFiling(item)

  }

  // loop through each filing history object looking for any with full accounts. 


  console.log(`${filings.length} companies have accounts, checking for full accounts...`)
  console.log(JSON.stringify(filings))

  for await (var item of filings) {
    // console.log(`item: ` + JSON.stringify(item))
    //console.log(item)
    if (item.accounts.description == 'accounts-with-accounts-type-total-exemption-full' || item.accounts.description == 'accounts-with-accounts-type-micro-entity' || item.accounts.description == 'accounts-with-accounts-type-full' || item.accounts.description == 'accounts-with-accounts-type-full-group' || item.accounts.description == 'accounts-with-accounts-type-medium-group' || item.accounts.description == 'accounts-with-accounts-type-interim' || item.accounts.description == 'accounts-with-accounts-type-medium' || item.accounts.description == 'accounts-with-accounts-type-group' || item.accounts.description == 'accounts-with-accounts-type-small' || item.accounts.description == 'accounts-with-accounts-type-small-group') {
      accounts.push(item)
      // console.log("added item to accounts: " + JSON.stringify(item))
    }
  }

  //console.log(accounts)

  if (accounts.length === 0) {
    console.log("None of these companies have full accounts!")
  }

  console.log(`${accounts.length} companies have filed full accounts, retrieving pdf's...`)
  //console.log("Accounts array: " + JSON.stringify(accounts))

  for await (var comp of accounts) {

    let headers = new Headers();

    //headers.append('Content-Type', 'text/xml')
    headers.append('Accept', 'application/pdf')
    headers.append('Authorization', 'Basic ' + btoa(key))

    //header object
    var obj = {
      method: 'GET',
      headers: headers
    }

    let metadata = comp.accounts.links.document_metadata
    //2. fetch the document metadata
    const meta = await fetch(metadata, obj)
    const metaResponse = await meta.json()
    //e.g https://frontend-doc-api.company-information.service.gov.uk/document/-HJYb4FxBNjuvqsQij3mrxsV4IxqjFcrsBtnTMNMBIk
    //console.log(metaResponse.links.document)

    let document = metaResponse.links.document

    //3. fetch the document 
    const doc = await fetch(document, obj)
    //e.g https://document-api.companieshouse.gov.uk/document/-HJYb4FxBNjuvqsQij3mrxsV4IxqjFcrsBtnTMNMBIk/content

    //console.log(`Getting account pdf for ${comp.company_name} from ${doc.url}...`)

    var dir = `./Accounts/Batch_${index}`;

    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }

    const doc_link = `${dir}/${comp.accounts.description.split("-").slice(-2).join("-")}_` + `${comp.name.replace(/\s/g, '_')}.pdf`

    async function insertData(comp) {
      try {
        for (const obj of items) {
          if (obj.number === comp.number) {
            obj.accounts_link = doc_link;

            const query = `INSERT INTO companies_full (number, name, status, address_1, address_2, town, county, country, postcode, incorporation_date, accounts_due_date, accounts_last_date, accounts_category, sic_code_1, sic_code_2, sic_code_3, sic_code_4, uri, id, flag, accounts, accounts_link) 
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`;
            const values = [obj.number, obj.name, obj.status, obj.address_1, obj.address_2, obj.town, obj.county, obj.country, obj.postcode, obj.incorporation_date, obj.accounts_due_date, obj.accounts_last_date, obj.accounts_category, obj.sic_code_1, obj.sic_code_2, obj.sic_code_3, obj.sic_code_4, obj.uri, obj.id, obj.flag, obj.accounts, obj.accounts_link];
            await db.query(query, values)
          }
        }
      } catch (err) {
        console.error('Error inserting data: ', err);
      }
    }

    insertData(comp)


    comp.accounts.links["document_link"] = doc_link
    //4. download the file by passing in the S3 url to the pdf and the folder you want to download to
    //no auth header needed in this fetch
    await downloadFile(doc.url, doc_link)
    //keys.push(i);
  }
  // INSERT these accounts objects into Postgres database...

  // console.log("Accounts array: " + JSON.stringify(accounts))
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


getAccounts()