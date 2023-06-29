import {
  KinesisClient,
  PutRecordsCommand,
  PutRecordsCommandOutput,
  PutRecordsRequestEntry,
} from '@aws-sdk/client-kinesis';
import { promisify } from 'util';
import { KinesisPutRecordsSend } from './types';
const sleep = promisify(setTimeout);

/**
 * Retries record failures within a Kinesis batch put
 */
export class KinesisRetrier implements KinesisPutRecordsSend {
  private readonly _retries: number;
  private readonly _retryBaseDelayMS: number;
  private readonly _kinesisClient: KinesisClient;

  /**
   * Creates a new KinesisRetrier
   * @param options KinesisRetrier options
   * @param options.kinesisClient - The KinesisClient instance
   * @param options.retries - Max number of retries for the batch - Default 5
   * @param options.retryBaseDelayMS - Delay, in milliseconds, for the first exponential backoff after a failure
   */
  constructor(options: {
    kinesisClient: KinesisClient;
    retries?: number;
    retryBaseDelayMS?: number;
  }) {
    const { kinesisClient, retries = 5, retryBaseDelayMS = 2000 } = options;

    this._kinesisClient = kinesisClient;
    this._retries = retries;
    this._retryBaseDelayMS = retryBaseDelayMS;
  }

  /**
   * Send a PutRecordsCommand and retry any failures with exponential backoff
   *
   * @param command
   * @returns
   */
  public async send(command: PutRecordsCommand): Promise<PutRecordsCommandOutput> {
    if (command.input.Records === undefined) {
      throw new Error('must pass in Records');
    }

    // Do the first send
    const result = await this._kinesisClient.send(command);

    if (result.Records === undefined) {
      throw new Error('kinesis put failed miserably - no records returned');
    }

    for (let retryCount = 0; retryCount < this._retries; retryCount++) {
      const indices: number[] = [];
      const toRetry: PutRecordsRequestEntry[] = [];

      // Return if there were no failures
      if (result.FailedRecordCount === 0 || result.FailedRecordCount === undefined) {
        return result;
      }

      // Exponential delay with jitter
      await sleep(this.delay(retryCount));

      // Loop through looking for failures
      for (let recordIndex = 0; recordIndex < result.Records?.length; recordIndex++) {
        const inputRecord = command.input.Records[recordIndex];
        const resultRecord = result.Records[recordIndex];
        if (resultRecord.ErrorCode !== undefined) {
          // Add failures to list and add their indices to a matching order list
          toRetry.push(inputRecord);
          indices.push(recordIndex);
        }
      }

      // Send the retries
      const retryResult = await this._kinesisClient.send(
        new PutRecordsCommand({ ...command.input, Records: toRetry }),
      );

      if (retryResult === undefined || retryResult.Records === undefined) {
        throw new Error('result or records were undefined on retry');
      }

      // Push up the failed record count
      result.FailedRecordCount = retryResult.FailedRecordCount;

      // Loop through retries
      for (
        let retryResultIndex = 0;
        retryResultIndex < retryResult.Records?.length;
        retryResultIndex++
      ) {
        // Get the original index
        const origIndex = indices[retryResultIndex];

        // Get the retry result
        const retryRecord = retryResult.Records[retryResultIndex];

        if (retryRecord.ErrorCode === undefined) {
          // Copy up to the total result
          result.Records[origIndex] = retryRecord;
        }

        // Note: We don't need to do anything with failures
        // They will still be marked as failures in the original result
        // We loop through the original result to reconstruct the retry list
      }
    }

    return result;
  }

  // Source: https://dev.solita.fi/2020/05/28/kinesis-streams-part-1.html
  private delay(attempt: number) {
    const exponentialDelay = this._retryBaseDelayMS * 2 ** attempt;
    return (
      Math.floor(Math.random() * (exponentialDelay - this._retryBaseDelayMS)) +
      this._retryBaseDelayMS
    );
  }
}

export class KinesisRetrierStatic {
  private static RETRIES = 5;
  private static RETRY_BASE_DELAY_MS = 2000;

  /**
   * Send a PutRecordsCommand and retry any failures with exponential backoff
   *
   * @param command
   * @returns
   */
  public static async putRecords(
    client: KinesisClient,
    command: PutRecordsCommand,
  ): Promise<PutRecordsCommandOutput> {
    if (command.input.Records === undefined) {
      throw new Error('must pass in Records');
    }

    // Do the first send
    const result = await client.send(command);

    if (result.Records === undefined) {
      throw new Error('kinesis put failed miserably - no records returned');
    }

    for (let retryCount = 0; retryCount < KinesisRetrierStatic.RETRIES; retryCount++) {
      const indices: number[] = [];
      const toRetry: PutRecordsRequestEntry[] = [];

      // Return if there were no failures
      if (result.FailedRecordCount === 0 || result.FailedRecordCount === undefined) {
        return result;
      }

      // Exponential delay with jitter
      await sleep(this.delay(retryCount));

      // Loop through looking for failures
      for (let recordIndex = 0; recordIndex < result.Records?.length; recordIndex++) {
        const inputRecord = command.input.Records[recordIndex];
        const resultRecord = result.Records[recordIndex];
        if (resultRecord.ErrorCode !== undefined) {
          // Add failures to list and add their indices to a matching order list
          toRetry.push(inputRecord);
          indices.push(recordIndex);
        }
      }

      // Send the retries
      const retryResult = await client.send(
        new PutRecordsCommand({ ...command.input, Records: toRetry }),
      );

      if (retryResult === undefined || retryResult.Records === undefined) {
        throw new Error('result or records were undefined on retry');
      }

      // Push up the failed record count
      result.FailedRecordCount = retryResult.FailedRecordCount;

      // Loop through retries
      for (
        let retryResultIndex = 0;
        retryResultIndex < retryResult.Records?.length;
        retryResultIndex++
      ) {
        // Get the original index
        const origIndex = indices[retryResultIndex];

        // Get the retry result
        const retryRecord = retryResult.Records[retryResultIndex];

        if (retryRecord.ErrorCode === undefined) {
          // Copy up to the total result
          result.Records[origIndex] = retryRecord;
        }

        // Note: We don't need to do anything with failures
        // They will still be marked as failures in the original result
        // We loop through the original result to reconstruct the retry list
      }
    }

    return result;
  }

  // Source: https://dev.solita.fi/2020/05/28/kinesis-streams-part-1.html
  private static delay(attempt: number) {
    const exponentialDelay = KinesisRetrierStatic.RETRY_BASE_DELAY_MS * 2 ** attempt;
    return (
      Math.floor(Math.random() * (exponentialDelay - KinesisRetrierStatic.RETRY_BASE_DELAY_MS)) +
      KinesisRetrierStatic.RETRY_BASE_DELAY_MS
    );
  }
}
