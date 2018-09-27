/**
 * This function should be registered to bucket events on deploy
 * See: https://cloud.google.com/functions/docs/calling/storage
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} callback The callback function.
 */
exports.gcsToBigQuery = (event, callback) => {
  const file = event.data;

  console.log(`  Event ${event.eventId}`);
  console.log(`  Event Type: ${event.eventType}`);
  console.log(`  Bucket: ${file.bucket}`);
  console.log(`  File: ${file.name}`);
  console.log(`  Metageneration: ${file.metageneration}`);
  console.log(`  Created: ${file.timeCreated}`);
  console.log(`  Updated: ${file.updated}`);
  const Storage = require('@google-cloud/storage');
  const BigQuery = require('@google-cloud/bigquery');
  const bigquery = new BigQuery();
  const dataset = bigquery.dataset('test_data'); // Dataset must already exist in BigQuery
  const table = dataset.table('func_load');

  // see all load job options: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load
  const metadata = {
    sourceFormat: 'CSV',
    skipLeadingRows: 1,
    // autodetect schema if values can be interprated
    autodetect: true,
    // schema: json of table schema
    // Example
    // schema: {
    //     fields: [
    //         {
    //             "description": "FullName",
    //             "mode": "NULLABLE",
    //             "name": "Roads_FULLNAME",
    //             "type": "STRING"
    //         },
    //         {
    //             "description": "LeftFromAddress",
    //             "mode": "NULLABLE",
    //             "name": "Roads_FROMADDR_L",
    //             "type": "INT64"
    //         }
    //     ]
    // }

    // Set the write disposition to append to an existing table.
    writeDisposition: 'WRITE_APPEND',
    createDisposition: "CREATE_IF_NEEDED"
  };

  const storage = new Storage({
    projectId: 'my-project-id' // GCP project id 
  });
  const data = storage.bucket(file.bucket).file(file.name);
  // WARNING this load job does not poll for job completion in order to limit function run time
  // Load job errors must be discovered from Stackdriver or the BigQuery api.
  table.createLoadJob(data, metadata,
      function(err, job) {
      if (err) {
        console.error("Something went wrong with submitting the BigQuery job. Error: ", err);
        callback();
      }
      else{
      console.log("BigQuery job successfully submitted");
      callback();
    }
    });
};
