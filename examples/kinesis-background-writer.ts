/* eslint-disable no-console */
import * as kinesis from '@aws-sdk/client-kinesis';
import { KinesisBackgroundWriter, KinesisRetrier } from '@shutterstock/kinesis-helpers';

const kinesisClient = new kinesis.KinesisClient({});
const { KINESIS_STREAM_NAME = 'kinesis-helpers-test-stream', RECORDS_TO_WRITE = '40000' } =
  process.env;
const RECORDS_TO_WRITE_NUM = parseInt(RECORDS_TO_WRITE, 10);
const RECORDS_PER_BATCH = 500;

async function main() {
  // Use a KinesisRetrier so that we do not get throttling exceptions
  // within the background batch writes
  const kinesisRetrier = new KinesisRetrier({
    kinesisClient,
  });

  const backgroundWriter = new KinesisBackgroundWriter({
    kinesisClient: kinesisRetrier,
    concurrency: 4,
  });

  const records: kinesis.PutRecordsCommandInput = {
    StreamName: KINESIS_STREAM_NAME,
    Records: [],
  };

  // Thanks TypeScript?  I guess? The value is assigned above but
  // we are getting "possibly undefined" in the loop below
  records.Records = [];

  for (let i = 0; i < RECORDS_PER_BATCH; i++) {
    records.Records.push({
      Data: Buffer.from('123', 'utf-8'),
      PartitionKey: '123',
    });
  }

  // Log how many records we will be writing
  console.log(`Writing ${RECORDS_TO_WRITE_NUM} records to ${KINESIS_STREAM_NAME}`);

  // Send a whole lot of records so we start getting throttled within the batches
  for (let i = 0; i < RECORDS_TO_WRITE_NUM; i += RECORDS_PER_BATCH) {
    // Note how many records we are adding and how long it took
    console.time(`Adding ${i} to ${i + RECORDS_PER_BATCH} records took`);
    await backgroundWriter.send(new kinesis.PutRecordsCommand(records));
    console.timeEnd(`Adding ${i} to ${i + RECORDS_PER_BATCH} records took`);
  }

  // Need to wait until the backgroundWriter is idle (has finished any pending requests)
  console.time('Waiting for backgroundWriter to be idle');
  await backgroundWriter.onIdle();
  console.timeEnd('Waiting for backgroundWriter to be idle');

  // If there were any errors, log them
  console.log('Number of errors:', backgroundWriter.errors.length);
  backgroundWriter.errors.forEach((error) => {
    console.error(error);
  });
}

void main();
