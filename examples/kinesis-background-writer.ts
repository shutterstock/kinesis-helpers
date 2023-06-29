import * as kinesis from '@aws-sdk/client-kinesis';
import { KinesisBackgroundWriter, KinesisRetrier } from '@shutterstock/kinesis-helpers';

const kinesisClient = new kinesis.KinesisClient({});
const { KINESIS_STREAM_NAME = 'kinesis-helpers-test-stream', RECORDS_TO_WRITE = '1000000' } =
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

  // Send a whole lot of records so we start getting throttled within the batches
  for (let i = 0; i < RECORDS_TO_WRITE_NUM; i += RECORDS_PER_BATCH) {
    await backgroundWriter.send(new kinesis.PutRecordsCommand(records));
  }

  // Need to wait until the backgroundWriter is idle (has finished any pending requests)
  await backgroundWriter.onIdle();

  // TODO: If there were any errors, log them
}

void main();
