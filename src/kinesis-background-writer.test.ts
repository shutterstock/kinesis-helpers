/* eslint-disable @typescript-eslint/no-non-null-assertion */
//kinesis/retrier.test.ts
/// <reference types="jest" />
import { promisify } from 'util';
import * as kinesis from '@aws-sdk/client-kinesis';
import { KinesisRetrier } from './kinesis-retrier';
import { KinesisBackgroundWriter, KinesisBackgroundWriterError } from './kinesis-background-writer';
import { mockClient, AwsClientStub } from 'aws-sdk-client-mock';
import { PutRecordsCommandOutput } from '@aws-sdk/client-kinesis';

const sleep = promisify(setTimeout);

describe('KinesisBackgroundWriter', () => {
  const kinesisClient: AwsClientStub<kinesis.KinesisClient> = mockClient(kinesis.KinesisClient);
  let kinesisRetrier: KinesisRetrier;

  beforeEach(() => {
    jest.resetAllMocks();
    kinesisClient.reset();
    kinesisRetrier = new KinesisRetrier({
      kinesisClient: kinesisClient as unknown as kinesis.KinesisClient,
      retryBaseDelayMS: 100,
    });
  });

  it('single success works - w/ retrier', async () => {
    const backgroundWriter = new KinesisBackgroundWriter({ kinesisClient: kinesisRetrier });
    const record: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        {
          Data: Buffer.from('123', 'utf-8'),
          PartitionKey: '123',
        },
      ],
    };

    kinesisClient.on(kinesis.PutRecordsCommand, record).resolves({
      Records: [record as kinesis.PutRecordsResultEntry],
    });

    await backgroundWriter.send(new kinesis.PutRecordsCommand(record));

    // Need to wait until the backgroundWriter is idle (has finished any pending requests)
    expect(backgroundWriter.isIdle).toBe(false);
    await backgroundWriter.onIdle();
    expect(backgroundWriter.isIdle).toBe(true);

    expect(backgroundWriter.errors.length).toBe(0);

    expect(kinesisClient.calls().length).toBe(1);
  });

  it('single success works - w/ retrier and FailedRecordCount = 0', async () => {
    const backgroundWriter = new KinesisBackgroundWriter({ kinesisClient: kinesisRetrier });
    const record: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        {
          Data: Buffer.from('123', 'utf-8'),
          PartitionKey: '123',
        },
      ],
    };

    kinesisClient.on(kinesis.PutRecordsCommand, record).resolves({
      Records: [record as kinesis.PutRecordsResultEntry],
      FailedRecordCount: 0,
    });

    await backgroundWriter.send(new kinesis.PutRecordsCommand(record));

    // Need to wait until the backgroundWriter is idle (has finished any pending requests)
    expect(backgroundWriter.isIdle).toBe(false);
    await backgroundWriter.onIdle();
    expect(backgroundWriter.isIdle).toBe(true);

    expect(backgroundWriter.errors.length).toBe(0);

    expect(kinesisClient.calls().length).toBe(1);
  });

  it('multiple success works - concurrency 1, w/ retrier', async () => {
    const backgroundWriter = new KinesisBackgroundWriter({
      kinesisClient: kinesisRetrier,
      concurrency: 1,
    });
    const record: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        {
          Data: Buffer.from('123', 'utf-8'),
          PartitionKey: '123',
        },
      ],
    };

    kinesisClient.on(kinesis.PutRecordsCommand, record).resolves({
      Records: [record as kinesis.PutRecordsResultEntry],
    });

    await backgroundWriter.send(new kinesis.PutRecordsCommand(record));
    await backgroundWriter.send(new kinesis.PutRecordsCommand(record));

    expect(kinesisClient.calls().length).toBe(2);

    // Need to wait until the backgroundWriter is idle (has finished any pending requests)
    expect(backgroundWriter.isIdle).toBe(false);
    await backgroundWriter.onIdle();
    expect(backgroundWriter.isIdle).toBe(true);

    expect(kinesisClient.calls().length).toBe(2);

    expect(backgroundWriter.errors.length).toBe(0);
  });

  it('concurrency 4 sends 4 concurrently then waits', async () => {
    const sleepDurationMs = 500;
    const kinesisSend = jest.fn().mockImplementation(async () => {
      await sleep(sleepDurationMs);
      return {
        FailedRecordCount: 0,
        Records: [],
        $metadata: {
          attempts: 1,
        },
      } as PutRecordsCommandOutput;
    });

    const backgroundWriter = new KinesisBackgroundWriter({
      kinesisClient: {
        send: kinesisSend,
      },
      concurrency: 4,
    });
    const record: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        {
          Data: Buffer.from('123', 'utf-8'),
          PartitionKey: '123',
        },
      ],
    };

    // First 4 added should not wait at all
    const startTime = Date.now();
    await backgroundWriter.send(new kinesis.PutRecordsCommand(record));
    await backgroundWriter.send(new kinesis.PutRecordsCommand(record));
    await backgroundWriter.send(new kinesis.PutRecordsCommand(record));
    await backgroundWriter.send(new kinesis.PutRecordsCommand(record));
    expect(Date.now() - startTime).toBeLessThan(sleepDurationMs);

    expect(kinesisSend.mock.calls.length).toBe(4);

    // Next one added should have had to wait for at least one wait period
    await backgroundWriter.send(new kinesis.PutRecordsCommand(record));

    expect(kinesisSend.mock.calls.length).toBe(5);

    expect(Date.now() - startTime).toBeGreaterThanOrEqual(sleepDurationMs);

    // Need to wait until the backgroundWriter is idle (has finished any pending requests)
    expect(backgroundWriter.isIdle).toBe(false);
    await backgroundWriter.onIdle();
    expect(backgroundWriter.isIdle).toBe(true);

    expect(Date.now() - startTime).toBeGreaterThanOrEqual(2 * sleepDurationMs);
    expect(Date.now() - startTime).toBeLessThan(2.2 * sleepDurationMs);

    expect(kinesisSend).toBeCalledTimes(5);

    expect(backgroundWriter.errors.length).toBe(0);
  });

  it('single fail at front, middle, and end works', async () => {
    const backgroundWriter = new KinesisBackgroundWriter({
      kinesisClient: kinesisRetrier,
      concurrency: 6,
    });
    for (let failIndex = 0; failIndex < 3; failIndex++) {
      const records: kinesis.PutRecordsCommandInput = {
        StreamName: 'some-stream',
        Records: [
          {
            Data: Buffer.from('123', 'utf-8'),
            PartitionKey: '123',
          },
          {
            Data: Buffer.from('456', 'utf-8'),
            PartitionKey: '456',
          },
          {
            Data: Buffer.from('789', 'utf-8'),
            PartitionKey: '789',
          },
        ],
      };
      const recordsRetrySucceed: kinesis.PutRecordsCommandInput = {
        StreamName: 'some-stream',
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        Records: [records.Records![failIndex] as kinesis.PutRecordsRequestEntry],
      };

      const results: kinesis.PutRecordsResultEntry[] = [];
      if (records.Records !== undefined) {
        records.Records.map((value) => {
          results.push({ ...value } as kinesis.PutRecordsResultEntry);
        });

        // Set first record to fail
        results[failIndex].ErrorCode = 'ProvisionedThroughputExceededException';
      }

      const resultsRetrySucceed: kinesis.PutRecordsResultEntry[] = [];
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      resultsRetrySucceed.push(records.Records![failIndex] as kinesis.PutRecordsResultEntry);

      kinesisClient
        .on(kinesis.PutRecordsCommand, records)
        .resolves({
          FailedRecordCount: 1,
          Records: results,
        })
        // On the second callback we'll only get 1 record passed in... let it succeed this time
        .on(kinesis.PutRecordsCommand, recordsRetrySucceed)
        .resolves({ Records: resultsRetrySucceed });

      // Send the records
      await backgroundWriter.send(new kinesis.PutRecordsCommand(records));
    }

    // Need to wait until the backgroundWriter is idle (has finished any pending requests)
    expect(backgroundWriter.isIdle).toBe(false);
    await backgroundWriter.onIdle();
    expect(backgroundWriter.isIdle).toBe(true);

    expect(kinesisClient.calls().length).toBe(6);

    // The background writer should not see any errors
    expect(backgroundWriter.errors.length).toBe(0);
  }, 20000);

  it('correct failed records array if ProvisionedThroughputExceededException on 1 of many records', async () => {
    const backgroundWriter = new KinesisBackgroundWriter({
      kinesisClient: kinesisRetrier,
      concurrency: 2,
    });
    const records: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        {
          Data: Buffer.from('123', 'utf-8'),
          PartitionKey: '123',
        },
        {
          Data: Buffer.from('456', 'utf-8'),
          PartitionKey: '456',
        },
        {
          Data: Buffer.from('789', 'utf-8'),
          PartitionKey: '789',
        },
      ],
    };
    const recordsAlwaysFails: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: records.Records?.slice(0, 1),
    };

    const resultsFirstCall: kinesis.PutRecordsResultEntry[] = [];
    if (records.Records !== undefined) {
      records.Records.map((value) => {
        resultsFirstCall.push({ ...value } as kinesis.PutRecordsResultEntry);
      });

      // Set 1 records to fail
      resultsFirstCall[0].ErrorCode = 'ProvisionedThroughputExceededException';
    }
    const resultsAlwaysFails: kinesis.PutRecordsResultEntry[] = [];
    if (recordsAlwaysFails.Records !== undefined) {
      recordsAlwaysFails.Records.map((value) => {
        resultsAlwaysFails.push({ ...value } as kinesis.PutRecordsResultEntry);
      });

      // Set 1 records to fail
      resultsAlwaysFails[0].ErrorCode = 'ProvisionedThroughputExceededException';
    }

    kinesisClient
      .onAnyCommand()
      .rejects()
      .on(kinesis.PutRecordsCommand, records)
      .resolvesOnce({ FailedRecordCount: 1, Records: resultsFirstCall })
      .rejects()
      .on(kinesis.PutRecordsCommand, recordsAlwaysFails)
      .resolves({ FailedRecordCount: 1, Records: resultsAlwaysFails });

    await backgroundWriter.send(new kinesis.PutRecordsCommand(records));

    // Need to wait until the backgroundWriter is idle (has finished any pending requests)
    expect(backgroundWriter.isIdle).toBe(false);
    await backgroundWriter.onIdle();
    expect(backgroundWriter.isIdle).toBe(true);

    expect(backgroundWriter.errors.length).toBe(1);
    expect(backgroundWriter.errors[0]).toBeInstanceOf(KinesisBackgroundWriterError);
    const typedErrors = backgroundWriter.errors.map(
      (error) => error as KinesisBackgroundWriterError,
    );
    expect(typedErrors[0].input).toEqual(records.Records![0]);
    expect(typedErrors[0].result).toEqual(resultsFirstCall[0]);
  }, 60000);

  it('send returns after retries if all records always return ProvisionedThroughputExceededException', async () => {
    const backgroundWriter = new KinesisBackgroundWriter({
      kinesisClient: kinesisRetrier,
      concurrency: 2,
    });
    const records: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        {
          Data: Buffer.from('123', 'utf-8'),
          PartitionKey: '123',
        },
        {
          Data: Buffer.from('456', 'utf-8'),
          PartitionKey: '456',
        },
        {
          Data: Buffer.from('789', 'utf-8'),
          PartitionKey: '789',
        },
      ],
    };

    const results: kinesis.PutRecordsResultEntry[] = [];
    if (records.Records !== undefined) {
      records.Records.map((value) => {
        results.push({ ...value } as kinesis.PutRecordsResultEntry);
      });

      // Set all records to fail
      results[0].ErrorCode = 'ProvisionedThroughputExceededException';
      results[1].ErrorCode = 'ProvisionedThroughputExceededException';
      results[2].ErrorCode = 'ProvisionedThroughputExceededException';
    }

    kinesisClient.onAnyCommand().rejects().on(kinesis.PutRecordsCommand, records).resolves({
      FailedRecordCount: 3,
      Records: results,
    });

    await backgroundWriter.send(new kinesis.PutRecordsCommand(records));
    await backgroundWriter.send(new kinesis.PutRecordsCommand(records));

    // Need to wait until the backgroundWriter is idle (has finished any pending requests)
    expect(backgroundWriter.isIdle).toBe(false);
    await backgroundWriter.onIdle();
    expect(backgroundWriter.isIdle).toBe(true);

    expect(backgroundWriter.errors.length).toBe(6);
    expect(backgroundWriter.errors[0]).toBeInstanceOf(KinesisBackgroundWriterError);
    expect(backgroundWriter.errors[1]).toBeInstanceOf(KinesisBackgroundWriterError);
    expect(backgroundWriter.errors[2]).toBeInstanceOf(KinesisBackgroundWriterError);
    expect(backgroundWriter.errors[3]).toBeInstanceOf(KinesisBackgroundWriterError);
    expect(backgroundWriter.errors[4]).toBeInstanceOf(KinesisBackgroundWriterError);
    expect(backgroundWriter.errors[5]).toBeInstanceOf(KinesisBackgroundWriterError);
    const typedErrors = backgroundWriter.errors.map(
      (error) => error as KinesisBackgroundWriterError,
    );
    expect(typedErrors[0].input).toEqual(records.Records![0]);
    expect(typedErrors[0].result).toEqual(results[0]);
    expect(typedErrors[3].input).toEqual(records.Records![0]);
    expect(typedErrors[3].result).toEqual(results[0]);
  }, 60000);
});
