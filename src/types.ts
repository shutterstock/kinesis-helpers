import { PutRecordsCommand, PutRecordsCommandOutput } from '@aws-sdk/client-kinesis';

export interface KinesisPutRecordsSend {
  send(command: PutRecordsCommand): Promise<PutRecordsCommandOutput>;
}
