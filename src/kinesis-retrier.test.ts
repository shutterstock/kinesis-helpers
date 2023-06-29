/* eslint-disable @typescript-eslint/no-non-null-assertion */
//kinesis/retrier.test.ts
/// <reference types="jest" />
import * as kinesis from '@aws-sdk/client-kinesis';
import { KinesisRetrierStatic, KinesisRetrier } from './kinesis-retrier';
import { mockClient, AwsClientStub } from 'aws-sdk-client-mock';

describe('KinesisRetrier', () => {
  const kinesisClient: AwsClientStub<kinesis.KinesisClient> = mockClient(kinesis.KinesisClient);
  let kinesisRetrier: KinesisRetrier;

  beforeEach(() => {
    jest.resetAllMocks();
    kinesisClient.reset();
    kinesisRetrier = new KinesisRetrier({
      kinesisClient: new kinesis.KinesisClient({}),
      retryBaseDelayMS: 10,
    });
  });

  it('single success works', async () => {
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
    const result = await kinesisRetrier.send(new kinesis.PutRecordsCommand(record));

    expect(result).toBeDefined();
    expect(result.FailedRecordCount).toBeUndefined();
    expect(result.Records?.length).toBe(1);
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(result.Records![0]!.ErrorCode).toBeUndefined();
  });

  it('send returns after retries if all records always return ProvisionedThroughputExceededException', async () => {
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

    const result = await kinesisRetrier.send(new kinesis.PutRecordsCommand(records));
    expect(result.FailedRecordCount).toBe(3);
    expect(result.Records).toMatchSnapshot();
    expect(kinesisClient.calls().length).toBe(6);
    expect(result.Records?.length).toBe(3);
    expect(result.Records![0]!.ErrorCode).toBe('ProvisionedThroughputExceededException');
    expect(result.Records![1]!.ErrorCode).toBe('ProvisionedThroughputExceededException');
    expect(result.Records![2]!.ErrorCode).toBe('ProvisionedThroughputExceededException');
  }, 60000);

  it('send rethrows any underlying KinesisClient.send exception', async () => {
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

    kinesisClient.onAnyCommand().rejects(new Error('some AWS client error'));

    await expect(async () =>
      kinesisRetrier.send(new kinesis.PutRecordsCommand(records)),
    ).rejects.toThrow('some AWS client error');
  }, 60000);

  it('single fail at front, middle, and end works', async () => {
    for (let failIndex = 0; failIndex < 3; failIndex++) {
      kinesisClient.reset();
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
      const result = await kinesisRetrier.send(new kinesis.PutRecordsCommand(records));
      expect(result.FailedRecordCount).toBeUndefined();
      expect(result.Records).toMatchSnapshot();
      expect(kinesisClient.calls().length).toBe(2);
      expect(result.Records?.length).toBe(3);
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      expect(result.Records![failIndex]!.ErrorCode).toBeUndefined();
    }
  }, 20000);

  it('multi fail works', async () => {
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
    const recordsRetrySecond: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![0] as kinesis.PutRecordsRequestEntry,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![1] as kinesis.PutRecordsRequestEntry,
      ],
    };
    const recordsRetryThird: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![1] as kinesis.PutRecordsRequestEntry,
      ],
    };

    const results: kinesis.PutRecordsResultEntry[] = [];
    if (records.Records !== undefined) {
      records.Records.map((value) => {
        results.push({ ...value } as kinesis.PutRecordsResultEntry);
      });

      // Set first records to fail
      results[0].ErrorCode = 'ProvisionedThroughputExceededException';
      results[1].ErrorCode = 'ProvisionedThroughputExceededException';
    }

    const resultsRetrySecond: kinesis.PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![0] as kinesis.PutRecordsResultEntry) });
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![1] as kinesis.PutRecordsResultEntry) });
    resultsRetrySecond[1].ErrorCode = 'ProvisionedThroughputExceededException';

    const resultsRetryThird: kinesis.PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetryThird.push(records.Records![1] as kinesis.PutRecordsResultEntry);

    kinesisClient
      .on(kinesis.PutRecordsCommand, records)
      .resolves({
        FailedRecordCount: 2,
        Records: results,
      })
      // On the second callback we'll get 2 record passed in... let 1 succeed this time
      .on(kinesis.PutRecordsCommand, recordsRetrySecond)
      .resolves({ FailedRecordCount: 1, Records: resultsRetrySecond })
      // On the third callback we'll only get 1 record passed in... let it succeed this time
      .on(kinesis.PutRecordsCommand, recordsRetryThird)
      .resolves({ Records: resultsRetryThird });

    // Send the records
    const result = await kinesisRetrier.send(new kinesis.PutRecordsCommand(records));
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
    const recordsRetrySecond: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![0] as kinesis.PutRecordsRequestEntry,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![1] as kinesis.PutRecordsRequestEntry,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![2] as kinesis.PutRecordsRequestEntry,
      ],
    };
    const recordsRetryThird: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![2] as kinesis.PutRecordsRequestEntry,
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

    const resultsRetrySecond: kinesis.PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![0] as kinesis.PutRecordsResultEntry) });
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![1] as kinesis.PutRecordsResultEntry) });
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![2] as kinesis.PutRecordsResultEntry) });
    resultsRetrySecond[2].ErrorCode = 'ProvisionedThroughputExceededException';

    const resultsRetryThird: kinesis.PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetryThird.push(records.Records![2] as kinesis.PutRecordsResultEntry);

    kinesisClient
      .on(kinesis.PutRecordsCommand, records)
      .resolves({
        FailedRecordCount: 3,
        Records: results,
      })
      // On the second callback we'll get 2 record passed in... let 1 succeed this time
      .on(kinesis.PutRecordsCommand, recordsRetrySecond)
      .resolves({ FailedRecordCount: 2, Records: resultsRetrySecond })
      // On the third callback we'll only get 1 record passed in... let it succeed this time
      .on(kinesis.PutRecordsCommand, recordsRetryThird)
      .resolves({ Records: resultsRetryThird });

    // Send the records
    const result = await kinesisRetrier.send(new kinesis.PutRecordsCommand(records));
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
  const kinesisClient: AwsClientStub<kinesis.KinesisClient> = mockClient(kinesis.KinesisClient);

  beforeEach(() => {
    jest.resetAllMocks();
    kinesisClient.reset();
  });

  it('single success works', async () => {
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
    const result = await KinesisRetrierStatic.putRecords(
      kinesisClient as unknown as kinesis.KinesisClient,
      new kinesis.PutRecordsCommand(record),
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
      const result = await KinesisRetrierStatic.putRecords(
        kinesisClient as unknown as kinesis.KinesisClient,
        new kinesis.PutRecordsCommand(records),
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
    const recordsRetrySecond: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![0] as kinesis.PutRecordsRequestEntry,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![1] as kinesis.PutRecordsRequestEntry,
      ],
    };
    const recordsRetryThird: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![1] as kinesis.PutRecordsRequestEntry,
      ],
    };

    const results: kinesis.PutRecordsResultEntry[] = [];
    if (records.Records !== undefined) {
      records.Records.map((value) => {
        results.push({ ...value } as kinesis.PutRecordsResultEntry);
      });

      // Set first records to fail
      results[0].ErrorCode = 'ProvisionedThroughputExceededException';
      results[1].ErrorCode = 'ProvisionedThroughputExceededException';
    }

    const resultsRetrySecond: kinesis.PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![0] as kinesis.PutRecordsResultEntry) });
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![1] as kinesis.PutRecordsResultEntry) });
    resultsRetrySecond[1].ErrorCode = 'ProvisionedThroughputExceededException';

    const resultsRetryThird: kinesis.PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetryThird.push(records.Records![1] as kinesis.PutRecordsResultEntry);

    kinesisClient
      .on(kinesis.PutRecordsCommand, records)
      .resolves({
        FailedRecordCount: 2,
        Records: results,
      })
      // On the second callback we'll get 2 record passed in... let 1 succeed this time
      .on(kinesis.PutRecordsCommand, recordsRetrySecond)
      .resolves({ FailedRecordCount: 1, Records: resultsRetrySecond })
      // On the third callback we'll only get 1 record passed in... let it succeed this time
      .on(kinesis.PutRecordsCommand, recordsRetryThird)
      .resolves({ Records: resultsRetryThird });

    // Send the records
    const result = await KinesisRetrierStatic.putRecords(
      kinesisClient as unknown as kinesis.KinesisClient,
      new kinesis.PutRecordsCommand(records),
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
    const recordsRetrySecond: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![0] as kinesis.PutRecordsRequestEntry,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![1] as kinesis.PutRecordsRequestEntry,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![2] as kinesis.PutRecordsRequestEntry,
      ],
    };
    const recordsRetryThird: kinesis.PutRecordsCommandInput = {
      StreamName: 'some-stream',
      Records: [
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        records.Records![2] as kinesis.PutRecordsRequestEntry,
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

    const resultsRetrySecond: kinesis.PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![0] as kinesis.PutRecordsResultEntry) });
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![1] as kinesis.PutRecordsResultEntry) });
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetrySecond.push({ ...(records.Records![2] as kinesis.PutRecordsResultEntry) });
    resultsRetrySecond[2].ErrorCode = 'ProvisionedThroughputExceededException';

    const resultsRetryThird: kinesis.PutRecordsResultEntry[] = [];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resultsRetryThird.push(records.Records![2] as kinesis.PutRecordsResultEntry);

    kinesisClient
      .on(kinesis.PutRecordsCommand, records)
      .resolves({
        FailedRecordCount: 3,
        Records: results,
      })
      // On the second callback we'll get 2 record passed in... let 1 succeed this time
      .on(kinesis.PutRecordsCommand, recordsRetrySecond)
      .resolves({ FailedRecordCount: 2, Records: resultsRetrySecond })
      // On the third callback we'll only get 1 record passed in... let it succeed this time
      .on(kinesis.PutRecordsCommand, recordsRetryThird)
      .resolves({ Records: resultsRetryThird });

    // Send the records
    const result = await KinesisRetrierStatic.putRecords(
      kinesisClient as unknown as kinesis.KinesisClient,
      new kinesis.PutRecordsCommand(records),
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
