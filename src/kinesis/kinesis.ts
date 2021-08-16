import { ReingestRecord } from '../index';
import { Kinesis } from 'aws-sdk';

export async function putRecordsToKinesisStream(
    streamName: string,
    region: string,
    records: ReingestRecord[],
    maxRetries: number,
    attempts = 0
): Promise<void> {
    const client = new Kinesis({ region: region });
    let failedRecords: ReingestRecord[] = [];
    let errorMessage;

    try {
        const response = await client.putRecords({ StreamName: streamName, Records: records }).promise();
        const codes: string[] = [];
        response.Records.forEach((record, index) => {
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
            await putRecordsToKinesisStream(streamName, region, failedRecords, maxRetries, attempts + 1);
        } else {
            console.warn(`Could not put records after ${maxRetries} attempts. ${errorMessage}`);
            throw new Error(`Could not put records after ${maxRetries} attempts. ${errorMessage}`);
        }
    }
}
