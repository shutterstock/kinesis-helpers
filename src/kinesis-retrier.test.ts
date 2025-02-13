/* eslint-disable @typescript-eslint/no-non-null-assertion */
//kinesis/retrier.test.ts
/// <reference types="jest" />
import {
  KinesisClient,
  PutRecordsCommandInput,
  PutRecordsCommand,
  PutRecordsRequestEntry,
  PutRecordsResultEntry,
} from '@aws-sdk/client-kinesis';
import { KinesisRetrierStatic, KinesisRetrier } from './kinesis-retrier';
import { mockClient, AwsClientStub } from 'aws-sdk-client-mock';

describe('KinesisRetrier', () => {
  const kinesisClient: AwsClientStub<KinesisClient> = mockClient(KinesisClient);
  let kinesisRetrier: KinesisRetrier;

  beforeEach(() => {
    jest.resetAllMocks();
    kinesisClient.reset();
    kinesisRetrier = new KinesisRetrier({
      kinesisClient: new KinesisClient({}),
      retryBaseDelayMS: 10,
    });
  });

  it('single success works', async () => {
    const record: PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        {
          Data: Buffer.from('123', 'utf-8'),
          PartitionKey: '123',
        },
      ],
    };

    kinesisClient.on(PutRecordsCommand, record).resolves({
      Records: [record as PutRecordsResultEntry],
    });
    const result = await kinesisRetrier.send(new PutRecordsCommand(record));

    expect(result).toBeDefined();
    expect(result.FailedRecordCount).toBeUndefined();
    expect(result.Records?.length).toBe(1);
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![0]!.ErrorCode).toBeUndefined();
  });

  it('send returns after retries if all records always return ProvisionedThroughputExceededException', async () => {
    const records: PutRecordsCommandInput = {
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

    const results: PutRecordsResultEntry[] = [];
    if (records.Records !== undefined) {
      records.Records.map((value) => {
        results.push({ ...value } as PutRecordsResultEntry);
      });

      // Set all records to fail
      results[0].ErrorCode = 'ProvisionedThroughputExceededException';
      results[1].ErrorCode = 'ProvisionedThroughputExceededException';
      results[2].ErrorCode = 'ProvisionedThroughputExceededException';
    }

    kinesisClient.onAnyCommand().rejects().on(PutRecordsCommand, records).resolves({
      FailedRecordCount: 3,
      Records: results,
    });

    const result = await kinesisRetrier.send(new PutRecordsCommand(records));
    expect(result.FailedRecordCount).toBe(3);
    expect(result.Records).toMatchSnapshot();
    expect(kinesisClient.calls().length).toBe(6);
    expect(result.Records?.length).toBe(3);
    expect(result.Records![0]!.ErrorCode).toBe('ProvisionedThroughputExceededException');
    expect(result.Records![1]!.ErrorCode).toBe('ProvisionedThroughputExceededException');
    expect(result.Records![2]!.ErrorCode).toBe('ProvisionedThroughputExceededException');
  }, 60000);

  it('send rethrows any underlying KinesisClient.send exception', async () => {
    const records: PutRecordsCommandInput = {
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

    kinesisClient.onAnyCommand().rejects(new Error('some AWS client error'));

    await expect(async () => kinesisRetrier.send(new PutRecordsCommand(records))).rejects.toThrow(
      'some AWS client error',
    );
  }, 60000);

  it('single fail at front, middle, and end works', async () => {
    for (let failIndex = 0; failIndex < 3; failIndex++) {
      kinesisClient.reset();
      const records: PutRecordsCommandInput = {
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
      const recordsRetrySucceed: PutRecordsCommandInput = {
        StreamName: 'some-stream',
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        Records: [records.Records![failIndex] as PutRecordsRequestEntry],
      };

      const results: PutRecordsResultEntry[] = [];
      if (records.Records !== undefined) {
        records.Records.map((value) => {
          results.push({ ...value } as PutRecordsResultEntry);
        });

        // Set first record to fail
        results[failIndex].ErrorCode = 'ProvisionedThroughputExceededException';
      }

      const resultsRetrySucceed: PutRecordsResultEntry[] = [];
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      resultsRetrySucceed.push(records.Records![failIndex] as PutRecordsResultEntry);

      kinesisClient
        .on(PutRecordsCommand, records)
        .resolves({
          FailedRecordCount: 1,
          Records: results,
        })
        // On the second callback we'll only get 1 record passed in... let it succeed this time
        .on(PutRecordsCommand, recordsRetrySucceed)
        .resolves({ Records: resultsRetrySucceed });

      // Send the records
      const result = await kinesisRetrier.send(new PutRecordsCommand(records));
      expect(result.FailedRecordCount).toBeUndefined();
      expect(result.Records).toMatchSnapshot();
      expect(kinesisClient.calls().length).toBe(2);
      expect(result.Records?.length).toBe(3);
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      expect(result.Records![failIndex]!.ErrorCode).toBeUndefined();
    }
  }, 20000);

  it('multi fail works', async () => {
    const records: PutRecordsCommandInput = {
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
    const recordsRetrySecond: PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![0] as PutRecordsRequestEntry,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![1] as PutRecordsRequestEntry,
      ],
    };
    const recordsRetryThird: PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![1] as PutRecordsRequestEntry,
      ],
    };

    const results: PutRecordsResultEntry[] = [];
    if (records.Records !== undefined) {
      records.Records.map((value) => {
        results.push({ ...value } as PutRecordsResultEntry);
      });

      // Set first records to fail
      results[0].ErrorCode = 'ProvisionedThroughputExceededException';
      results[1].ErrorCode = 'ProvisionedThroughputExceededException';
    }

    const resultsRetrySecond: PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![0] as PutRecordsResultEntry) });
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![1] as PutRecordsResultEntry) });
    resultsRetrySecond[1].ErrorCode = 'ProvisionedThroughputExceededException';

    const resultsRetryThird: PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetryThird.push(records.Records![1] as PutRecordsResultEntry);

    kinesisClient
      .on(PutRecordsCommand, records)
      .resolves({
        FailedRecordCount: 2,
        Records: results,
      })
      // On the second callback we'll get 2 record passed in... let 1 succeed this time
      .on(PutRecordsCommand, recordsRetrySecond)
      .resolves({ FailedRecordCount: 1, Records: resultsRetrySecond })
      // On the third callback we'll only get 1 record passed in... let it succeed this time
      .on(PutRecordsCommand, recordsRetryThird)
      .resolves({ Records: resultsRetryThird });

    // Send the records
    const result = await kinesisRetrier.send(new PutRecordsCommand(records));
    expect(result.FailedRecordCount).toBeUndefined();
    expect(result.Records).toMatchSnapshot();
    expect(kinesisClient.calls().length).toBe(3);
    expect(result.Records?.length).toBe(3);
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![0]!.ErrorCode).toBeUndefined();
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![1]!.ErrorCode).toBeUndefined();
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![2]!.ErrorCode).toBeUndefined();
  }, 20000);

  it('all fail initial works', async () => {
    const records: PutRecordsCommandInput = {
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
    const recordsRetrySecond: PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![0] as PutRecordsRequestEntry,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![1] as PutRecordsRequestEntry,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![2] as PutRecordsRequestEntry,
      ],
    };
    const recordsRetryThird: PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![2] as PutRecordsRequestEntry,
      ],
    };

    const results: PutRecordsResultEntry[] = [];
    if (records.Records !== undefined) {
      records.Records.map((value) => {
        results.push({ ...value } as PutRecordsResultEntry);
      });

      // Set all records to fail
      results[0].ErrorCode = 'ProvisionedThroughputExceededException';
      results[1].ErrorCode = 'ProvisionedThroughputExceededException';
      results[2].ErrorCode = 'ProvisionedThroughputExceededException';
    }

    const resultsRetrySecond: PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![0] as PutRecordsResultEntry) });
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![1] as PutRecordsResultEntry) });
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![2] as PutRecordsResultEntry) });
    resultsRetrySecond[2].ErrorCode = 'ProvisionedThroughputExceededException';

    const resultsRetryThird: PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetryThird.push(records.Records![2] as PutRecordsResultEntry);

    kinesisClient
      .on(PutRecordsCommand, records)
      .resolves({
        FailedRecordCount: 3,
        Records: results,
      })
      // On the second callback we'll get 2 record passed in... let 1 succeed this time
      .on(PutRecordsCommand, recordsRetrySecond)
      .resolves({ FailedRecordCount: 2, Records: resultsRetrySecond })
      // On the third callback we'll only get 1 record passed in... let it succeed this time
      .on(PutRecordsCommand, recordsRetryThird)
      .resolves({ Records: resultsRetryThird });

    // Send the records
    const result = await kinesisRetrier.send(new PutRecordsCommand(records));
    expect(result.FailedRecordCount).toBeUndefined();
    expect(result.Records).toMatchSnapshot();
    expect(kinesisClient.calls().length).toBe(2);
    expect(result.Records?.length).toBe(3);
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![0]!.ErrorCode).toBeUndefined();
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![1]!.ErrorCode).toBeUndefined();
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![2]!.ErrorCode).toBeUndefined();
  }, 20000);
});

describe('KinesisRetrierStatic', () => {
  const kinesisClient: AwsClientStub<KinesisClient> = mockClient(KinesisClient);

  beforeEach(() => {
    jest.resetAllMocks();
    kinesisClient.reset();
  });

  it('single success works', async () => {
    const record: PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        {
          Data: Buffer.from('123', 'utf-8'),
          PartitionKey: '123',
        },
      ],
    };

    kinesisClient.on(PutRecordsCommand, record).resolves({
      Records: [record as PutRecordsResultEntry],
    });
    const result = await KinesisRetrierStatic.putRecords(
      kinesisClient as unknown as KinesisClient,
      new PutRecordsCommand(record),
    );

    expect(result).toBeDefined();
    expect(result.FailedRecordCount).toBeUndefined();
    expect(result.Records?.length).toBe(1);
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![0]!.ErrorCode).toBeUndefined();
  });

  it('single fail at front, middle, and end works', async () => {
    for (let failIndex = 0; failIndex < 3; failIndex++) {
      kinesisClient.reset();
      const records: PutRecordsCommandInput = {
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
      const recordsRetrySucceed: PutRecordsCommandInput = {
        StreamName: 'some-stream',
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        Records: [records.Records![failIndex] as PutRecordsRequestEntry],
      };

      const results: PutRecordsResultEntry[] = [];
      if (records.Records !== undefined) {
        records.Records.map((value) => {
          results.push({ ...value } as PutRecordsResultEntry);
        });

        // Set first record to fail
        results[failIndex].ErrorCode = 'ProvisionedThroughputExceededException';
      }

      const resultsRetrySucceed: PutRecordsResultEntry[] = [];
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      resultsRetrySucceed.push(records.Records![failIndex] as PutRecordsResultEntry);

      kinesisClient
        .on(PutRecordsCommand, records)
        .resolves({
          FailedRecordCount: 1,
          Records: results,
        })
        // On the second callback we'll only get 1 record passed in... let it succeed this time
        .on(PutRecordsCommand, recordsRetrySucceed)
        .resolves({ Records: resultsRetrySucceed });

      // Send the records
      const result = await KinesisRetrierStatic.putRecords(
        kinesisClient as unknown as KinesisClient,
        new PutRecordsCommand(records),
      );
      expect(result.FailedRecordCount).toBeUndefined();
      expect(result.Records).toMatchSnapshot();
      expect(kinesisClient.calls().length).toBe(2);
      expect(result.Records?.length).toBe(3);
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      expect(result.Records![failIndex]!.ErrorCode).toBeUndefined();
    }
  }, 20000);

  it('multi fail works', async () => {
    const records: PutRecordsCommandInput = {
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
    const recordsRetrySecond: PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![0] as PutRecordsRequestEntry,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![1] as PutRecordsRequestEntry,
      ],
    };
    const recordsRetryThird: PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![1] as PutRecordsRequestEntry,
      ],
    };

    const results: PutRecordsResultEntry[] = [];
    if (records.Records !== undefined) {
      records.Records.map((value) => {
        results.push({ ...value } as PutRecordsResultEntry);
      });

      // Set first records to fail
      results[0].ErrorCode = 'ProvisionedThroughputExceededException';
      results[1].ErrorCode = 'ProvisionedThroughputExceededException';
    }

    const resultsRetrySecond: PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![0] as PutRecordsResultEntry) });
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![1] as PutRecordsResultEntry) });
    resultsRetrySecond[1].ErrorCode = 'ProvisionedThroughputExceededException';

    const resultsRetryThird: PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetryThird.push(records.Records![1] as PutRecordsResultEntry);

    kinesisClient
      .on(PutRecordsCommand, records)
      .resolves({
        FailedRecordCount: 2,
        Records: results,
      })
      // On the second callback we'll get 2 record passed in... let 1 succeed this time
      .on(PutRecordsCommand, recordsRetrySecond)
      .resolves({ FailedRecordCount: 1, Records: resultsRetrySecond })
      // On the third callback we'll only get 1 record passed in... let it succeed this time
      .on(PutRecordsCommand, recordsRetryThird)
      .resolves({ Records: resultsRetryThird });

    // Send the records
    const result = await KinesisRetrierStatic.putRecords(
      kinesisClient as unknown as KinesisClient,
      new PutRecordsCommand(records),
    );
    expect(result.FailedRecordCount).toBeUndefined();
    expect(result.Records).toMatchSnapshot();
    expect(kinesisClient.calls().length).toBe(3);
    expect(result.Records?.length).toBe(3);
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![0]!.ErrorCode).toBeUndefined();
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![1]!.ErrorCode).toBeUndefined();
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![2]!.ErrorCode).toBeUndefined();
  }, 20000);

  it('all fail initial works', async () => {
    const records: PutRecordsCommandInput = {
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
    const recordsRetrySecond: PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![0] as PutRecordsRequestEntry,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![1] as PutRecordsRequestEntry,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![2] as PutRecordsRequestEntry,
      ],
    };
    const recordsRetryThird: PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![2] as PutRecordsRequestEntry,
      ],
    };

    const results: PutRecordsResultEntry[] = [];
    if (records.Records !== undefined) {
      records.Records.map((value) => {
        results.push({ ...value } as PutRecordsResultEntry);
      });

      // Set all records to fail
      results[0].ErrorCode = 'ProvisionedThroughputExceededException';
      results[1].ErrorCode = 'ProvisionedThroughputExceededException';
      results[2].ErrorCode = 'ProvisionedThroughputExceededException';
    }

    const resultsRetrySecond: PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![0] as PutRecordsResultEntry) });
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![1] as PutRecordsResultEntry) });
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![2] as PutRecordsResultEntry) });
    resultsRetrySecond[2].ErrorCode = 'ProvisionedThroughputExceededException';

    const resultsRetryThird: PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetryThird.push(records.Records![2] as PutRecordsResultEntry);

    kinesisClient
      .on(PutRecordsCommand, records)
      .resolves({
        FailedRecordCount: 3,
        Records: results,
      })
      // On the second callback we'll get 2 record passed in... let 1 succeed this time
      .on(PutRecordsCommand, recordsRetrySecond)
      .resolves({ FailedRecordCount: 2, Records: resultsRetrySecond })
      // On the third callback we'll only get 1 record passed in... let it succeed this time
      .on(PutRecordsCommand, recordsRetryThird)
      .resolves({ Records: resultsRetryThird });

    // Send the records
    const result = await KinesisRetrierStatic.putRecords(
      kinesisClient as unknown as KinesisClient,
      new PutRecordsCommand(records),
    );
    expect(result.FailedRecordCount).toBeUndefined();
    expect(result.Records).toMatchSnapshot();
    expect(kinesisClient.calls().length).toBe(2);
    expect(result.Records?.length).toBe(3);
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![0]!.ErrorCode).toBeUndefined();
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![1]!.ErrorCode).toBeUndefined();
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![2]!.ErrorCode).toBeUndefined();
  }, 20000);
});
