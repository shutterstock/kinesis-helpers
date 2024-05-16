import { IterableQueueMapperSimple } from '@shutterstock/p-map-iterable';
import {
  PutRecordsCommand,
  PutRecordsRequestEntry,
  PutRecordsResultEntry,
} from '@aws-sdk/client-kinesis';
import { KinesisPutRecordsSend } from './types';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Errors = (string | { [key: string]: any } | Error | KinesisBackgroundWriterError)[];

export interface IKinesisBackgroundWriterError {
  readonly input: PutRecordsRequestEntry;
  readonly result: PutRecordsResultEntry;
}

/**
 * Single-record error from Kinesis PutRecordsCommand
 * The error and the input data are both returned
 */
export class KinesisBackgroundWriterError implements IKinesisBackgroundWriterError {
  public readonly input: PutRecordsRequestEntry;
  public readonly result: PutRecordsResultEntry;

  constructor(args: IKinesisBackgroundWriterError) {
    this.input = args.input;
    this.result = args.result;
  }
}

/**
 * Accepts payloads for writing to Kinesis in the background, up to a limit
 */
export class KinesisBackgroundWriter {
  private readonly _writer: IterableQueueMapperSimple<PutRecordsCommand>;
  private readonly _kinesisClient: KinesisPutRecordsSend;
  private readonly _errors: Errors = [];

  /**
   * Creates a new KinesisBackgroundWriter
   *
   * Allows up to `concurrency` payloads to be in progress before
   * `write` will block until a payload completes.
   *
   * @param options KinesisBackgroundWriter options
   */
  constructor(options: {
    /**
     * Number of payloads to accept for background writing before requiring the caller to wait for one to complete.
     * @defaultValue 4
     */
    concurrency?: number;

    /**
     * Required - Can be KinesisClient or KinesisRetrier
     */
    kinesisClient: KinesisPutRecordsSend;
  }) {
    const { concurrency = 4, kinesisClient } = options;

    this.worker = this.worker.bind(this);
    this._kinesisClient = kinesisClient;
    this._writer = new IterableQueueMapperSimple(this.worker, {
      concurrency,
    });
  }

  private async worker(command: PutRecordsCommand): Promise<void> {
    try {
      const results = await this._kinesisClient.send(command);

      // If there are error records, add them to the errors array
      if (results.FailedRecordCount && results.Records !== undefined) {
        for (let recordIndex = 0; recordIndex < results.Records.length; recordIndex++) {
          // Only copy records that failed
          if (results.Records[recordIndex].ErrorCode) {
            const result = results.Records[recordIndex];
            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            const input = command.input.Records![recordIndex];
            const error = new KinesisBackgroundWriterError({
              input,
              result,
            });
            this._errors.push(error);
          }
        }
      }
    } catch (error: any) {
      // We have to catch these and expose them instead of allowing IterableQueueMapperSimple to catch them
      // as it will hide them on an error collection that we do not expose to the caller
      this._errors.push(error);
    }
  }

  /*
   * Accumulated errors from background `send`'s
   */
  public get errors(): Errors {
    return this._errors;
  }

  /**
   * Accept a request for sending in the background if a concurrency slot is available.
   * Else, do not return until a concurrency slot is freed up.
   *
   * This provides concurrency background writes with back pressure to prevent
   * the caller from getting too far ahead.
   *
   * Individual PutRecordsCommand records that fail after retries are added to the `errors` property.
   *
   * MUST await `onIdle` for background `send`'s to finish
   *
   * SHOULD periodically check `errors` for any individual record failures from `send`'s
   *
   * @param command
   */
  public async send(command: PutRecordsCommand): Promise<void> {
    // Return immediately or wait for a slot to free up in the background writer
    await this._writer.enqueue(command);
  }

  /**
   * Wait for all background writes to finish.
   * MUST be called before exit to ensure no lost writes.
   */
  public async onIdle(): Promise<void> {
    // Indicate that we're done writing requests
    await this._writer.onIdle();
  }

  /**
   * @returns true if .onIdle() has been called and finished all background writes
   */
  public get isIdle(): boolean {
    return this._writer.isIdle;
  }
}
