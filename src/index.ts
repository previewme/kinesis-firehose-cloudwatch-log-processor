/*
For processing data sent to Firehose by Cloudwatch Logs subscription filters.
Cloudwatch Logs sends to Firehose records that look like this:
{
	"messageType": "DATA_MESSAGE",
	"owner": "123456789012",
	"logGroup": "log_group_name",
	"logStream": "log_stream_name",
	"subscriptionFilters": [
		"subscription_filter_name"
	],
	"logEvents": [
		{
			"id": "01234567890123456789012345678901234567890123456789012345",
			"timestamp": 1510109208016,
			"message": "log message 1"
		},
		{
			"id": "01234567890123456789012345678901234567890123456789012345",
			"timestamp": 1510109208017,
			"message": "log message 2"
		}
		...
	]
}
The data is additionally compressed with GZIP.
The code below will:
1) Gunzip the data
2) Parse the json
3) Set the result to Dropped for any record whose messageType is CONTROL_MESSAGE. Set the result to ProcessingFailed for
any record whose messageType is not DATA_MESSAGE, thus redirecting them to the processing error output. Such records do
not contain any log events. You can modify the code to set the result to Dropped instead to get rid of these records
completely.
4) For records whose messageType is DATA_MESSAGE, pass the Cloudwatch Log Data to the transformLogEvent method. You can
modify the transformLogEvent method to perform custom transformations on the log events.
5) Concatenate the result from (4) together and set the result as the data of the record returned to Firehose. Note that
this step will not add any delimiters. Delimiters should be appended by the logic within the transformLogEvent
method.
6) Any additional records which exceed 6MB will be re-ingested back into the source Kinesis stream or Firehose.
*/
import {
    CloudWatchLogsDecodedData,
    FirehoseTransformationEvent,
    FirehoseTransformationEventRecord,
    FirehoseTransformationResult,
    FirehoseTransformationResultRecord
} from 'aws-lambda';
import { processFirehoseRecord, transformLogEvent } from './cloudwatch/logs';
import { putRecordsToFirehoseStream } from './kinesis/firehose';
import { putRecordsToKinesisStream } from './kinesis/kinesis';
import { PartitionKey } from 'aws-sdk/clients/kinesis';

export interface ReingestRecord {
    Data: Buffer;
    PartitionKey: PartitionKey;
}

export interface ReingestRecords {
    [key: string]: ReingestRecord;
}

export interface ProcessedResult {
    responseRecords: FirehoseTransformationResultRecord[];
    reingestBatches: ReingestRecord[][];
    reingestRecordCount: number;
}

// 6000000 instead of 6291456 to leave ample headroom for the stuff we didn't account for
const MAX_DATA_SIZE = 6000000;
const MAX_BATCH_SIZE = 500;

export function createReingestRecord(record: FirehoseTransformationEventRecord, isSourceAStream: boolean): ReingestRecord {
    const reingestRecord: ReingestRecord = {
        Data: Buffer.from(record.data, 'base64'),
        PartitionKey: ''
    };

    if (isSourceAStream && record.kinesisRecordMetadata?.partitionKey) {
        reingestRecord.PartitionKey = record.kinesisRecordMetadata.partitionKey;
    }
    return reingestRecord;
}

export function processRecords(recordId: string, message: CloudWatchLogsDecodedData): FirehoseTransformationResultRecord {
    if ('CONTROL_MESSAGE' === message.messageType) {
        return {
            recordId: recordId,
            result: 'Dropped',
            data: ''
        };
    } else if ('DATA_MESSAGE' === message.messageType) {
        const data = transformLogEvent(message);
        const encodedData = Buffer.from(data).toString('base64');
        if (encodedData.length <= MAX_DATA_SIZE) {
            return {
                recordId: recordId,
                result: 'Ok',
                data: encodedData
            };
        }
    }
    return {
        recordId: recordId,
        result: 'ProcessingFailed',
        data: ''
    };
}

export function calculateRecordsToReingest(responseRecords: FirehoseTransformationResultRecord[], reingestData: ReingestRecords): ProcessedResult {
    let projectedSize = 0;
    let totalRecordsToBeReingested = 0;
    let recordsToReingest: Array<ReingestRecord> = [];
    const putRecordBatches = [];

    responseRecords.forEach((record: FirehoseTransformationResultRecord, index: number) => {
        // This increases the amount of data we reingest but allows us to handle when there is a large number of ProcessingFailed/Dropped records.
        projectedSize += record.recordId.length + record.result.length;
        if (record.result !== 'Ok') {
            return;
        }

        projectedSize += record.data.length;
        if (projectedSize > MAX_DATA_SIZE) {
            totalRecordsToBeReingested++;
            recordsToReingest.push(reingestData[record.recordId]);
            responseRecords[index].data = '';
            responseRecords[index].result = 'Dropped';
        }

        if (recordsToReingest.length === MAX_BATCH_SIZE) {
            putRecordBatches.push(recordsToReingest);
            recordsToReingest = [];
        }
    });

    if (recordsToReingest.length > 0) {
        putRecordBatches.push(recordsToReingest);
    }

    return {
        responseRecords: responseRecords,
        reingestBatches: putRecordBatches,
        reingestRecordCount: totalRecordsToBeReingested
    };
}

function reingestRecords(result: ProcessedResult, isSourceAStream: boolean, streamARN: string, recordCount: number) {
    const region = streamARN.split(':')[3];
    const streamName = streamARN.split('/')[1];

    let recordsReingestedSoFar = 0;
    if (result.reingestRecordCount > 0) {
        result.reingestBatches.forEach((batch: ReingestRecord[]) => {
            isSourceAStream ? putRecordsToKinesisStream(streamName, region, batch, 20) : putRecordsToFirehoseStream(streamName, region, batch, 20);
            recordsReingestedSoFar += batch.length;
            console.info('Reingested %s/%s records out of %s', recordsReingestedSoFar, result.reingestRecordCount, recordCount);
        });
    } else {
        console.info('No records to be reingested');
    }
}

/*
 * logEvent has this format:
 *
 * {
 *   "id": "01234567890123456789012345678901234567890123456789012345",
 *   "timestamp": 1510109208016,
 *   "message": "log message 1"
 * }
 *
 * The implementation creates a JSON representation of the Cloudwatch Log event.
 *
 * The result must be returned in a Promise.
 */
export async function handler(event: FirehoseTransformationEvent): Promise<FirehoseTransformationResult> {
    let isSourceAStream = false;
    let streamARN = event.deliveryStreamArn;
    if (event.sourceKinesisStreamArn !== undefined) {
        isSourceAStream = true;
        streamARN = event.sourceKinesisStreamArn;
    }
    const responseRecords: FirehoseTransformationResultRecord[] = [];
    const reingestData: ReingestRecords = {};
    event.records.forEach((record: FirehoseTransformationEventRecord) => {
        const message = processFirehoseRecord(record);
        responseRecords.push(processRecords(record.recordId, message));
        reingestData[record.recordId] = createReingestRecord(record, isSourceAStream);
    });

    const reingestResult = calculateRecordsToReingest(responseRecords, reingestData);
    reingestRecords(reingestResult, isSourceAStream, streamARN, event.records.length);
    return { records: reingestResult.responseRecords };
}
