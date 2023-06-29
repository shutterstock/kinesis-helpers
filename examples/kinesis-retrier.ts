import * as kinesis from '@aws-sdk/client-kinesis';
import { KinesisRetrierStatic } from '@shutterstock/kinesis-helpers';

const kinesisClient = new kinesis.KinesisClient({});
const { KINESIS_STREAM_NAME = 'kinesis-helpers-test-stream', RECORDS_TO_WRITE = '1000000' } =
  process.env;
const RECORDS_TO_WRITE_NUM = parseInt(RECORDS_TO_WRITE, 10);
const RECORDS_PER_BATCH = 500;

async function main() {
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
    const result = await KinesisRetrierStatic.putRecords(
      kinesisClient,
      new kinesis.PutRecordsCommand(records),
    );

    if (result.FailedRecordCount ?? 0 > 0) {
      throw new Error('this should not happen - we should get backoff retries on the batch puts');
    }
  }
}

void main();
