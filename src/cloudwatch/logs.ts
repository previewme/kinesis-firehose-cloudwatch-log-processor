import { FirehoseTransformationEventRecord } from 'aws-lambda/trigger/kinesis-firehose-transformation';
import { CloudWatchLogsDecodedData, CloudWatchLogsLogEvent } from 'aws-lambda';
import { gunzipSync } from 'zlib';

interface CloudwatchLog {
    awsAccountId: string;
    logGroup: string;
    logStream: string;
    id: string;
    timestamp: number;
    message: string;
}

export function processFirehoseRecord(record: FirehoseTransformationEventRecord): CloudWatchLogsDecodedData {
    try {
        const buffer = Buffer.from(record.data, 'base64');
        const data = gunzipSync(buffer).toString();
        return JSON.parse(data);
    } catch (e) {
        console.warn('Could not decode data', e.message);
    }
    return {
        owner: '',
        logStream: '',
        logGroup: '',
        messageType: '',
        subscriptionFilters: [],
        logEvents: []
    };
}

export function transformLogEvent(cloudwatchLogs: CloudWatchLogsDecodedData): string {
    let logs = '';
    cloudwatchLogs.logEvents.forEach((event: CloudWatchLogsLogEvent) => {
        const log: CloudwatchLog = {
            awsAccountId: cloudwatchLogs.owner,
            logGroup: cloudwatchLogs.logGroup,
            logStream: cloudwatchLogs.logStream,
            id: event.id,
            message: event.message,
            timestamp: event.timestamp
        };
        logs = logs.concat(JSON.stringify(log), '\n');
    });
    return logs;
}
