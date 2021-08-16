import { ReingestRecord } from '../index';
import { Firehose } from 'aws-sdk';

export async function putRecordsToFirehoseStream(
    streamName: string,
    region: string,
    records: ReingestRecord[],
    maxRetries: number,
    attempts = 0
): Promise<void> {
    const client = new Firehose({ region: region });
    let failedRecords: ReingestRecord[] = [];
    let errorMessage;

    try {
        const response = await client.putRecordBatch({ DeliveryStreamName: streamName, Records: records }).promise();
        const codes: string[] = [];
        response.RequestResponses.forEach((record, index) => {
            const code = record.ErrorCode;
            if (code) {
                codes.push(code);
                failedRecords.push(records[index]);
            }
        });
        errorMessage = `Individual error codes: ${codes}`;
    } catch (err) {
        errorMessage = err.message;
        failedRecords = records;
    }

    if (failedRecords.length > 0) {
        if (attempts < maxRetries) {
            console.info('Some records failed while calling PutRecordBatch, retrying. %s', errorMessage);
            await putRecordsToFirehoseStream(streamName, region, failedRecords, maxRetries, attempts + 1);
        } else {
            console.warn(`Could not put records after ${maxRetries} attempts. ${errorMessage}`);
            throw new Error(`Could not put records after ${maxRetries} attempts. ${errorMessage}`);
        }
    }
}
