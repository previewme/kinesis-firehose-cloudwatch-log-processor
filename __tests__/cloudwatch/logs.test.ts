import { FirehoseTransformationEventRecord } from 'aws-lambda/trigger/kinesis-firehose-transformation';
import { CloudWatchLogsDecodedData } from 'aws-lambda';
import { processFirehoseRecord, transformLogEvent } from '../../src/cloudwatch/logs';

describe('Cloudwatch Logs', function () {
    test('Ensure firehose data can be processed as Cloudwatch Logs data', () => {
        const payload: FirehoseTransformationEventRecord = {
            recordId: 'jest test',
            data: 'H4sIAAAAAAAAAJWRTWsbMRCG/8ueLZjRjL5yc9NNLnZDapemlFAkrTYstb3Lep0Qgv97x00KgTSHnAQzmkeP3nmqtmW/j3dl/TiU6qz6PF/Pfy3r1Wp+WVezqn/YlVHK2pK3Hr0Jxkt5099djv1hkE7uh0eVHzZqE7epiarb3fe/ixzDYVJoELRhssYQqsXLlEJ3jd8//biy4QYWz7jVNJa4/TDveQwV+qsada0v/HnthLg/pH0eu2Hq+t1Ft5nKuK/Ofn4EvnpDUAu7Xi6/LL9en3/z1e1f7fq+7KYT+qnqGrEnsi54AGS2wbHWxjCjoWAYGawmzawByIG3Dp0JzjOxsaI8dbKJKW4l1BcTdgg+zP5tSPCeQ/Bso/I+o+I2kUptjgrRlQyasslUHWdvZRwGJ4+HYJGCtiKgQTYKSJ4gODLgAkpFk3f0rkyA1zLGSsvoVsVCRTFakUkNqKxt1IyFc8T/y0gEmoHZo5a/W9HhU0TeWHMyIJaoQC6zDvC+DL6WSW3MqZSkiolJcWoalWybJSNIJTXcRgjV8fb4BwwLrNzwAgAA',
            approximateArrivalTimestamp: 1510254471091
        };

        const expected: CloudWatchLogsDecodedData = {
            owner: '263868185958',
            logGroup: 'copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L',
            messageType: 'DATA_MESSAGE',
            logStream: 'copy-cwl-lambda-invoke-input-1510254365531-LogStream1-18OE12E2F8CE7',
            subscriptionFilters: ['copy-cwl-lambda-invoke-input-1510254365531-SubscriptionFilter-L6TMMNMRQCU8'],
            logEvents: [
                {
                    id: '33679800144697422554415395414062324420037086717597843456',
                    message: '8499846a-88c1-4fb3-bfca-117ec023c5c3',
                    timestamp: 1510254471089
                },
                {
                    id: '33679800144719723299613926037203860138309735079103823873',
                    message: '5615152f-ae3e-4163-bbd0-c26a241e4ca1',
                    timestamp: 1510254471090
                },
                {
                    id: '33679800144742024044812456660345395856582383440609804290',
                    message: 'bfacbeeb-e5ab-4bdd-b6fc-4f0bebd4fa09',
                    timestamp: 1510254471091
                }
            ]
        };

        const actual = processFirehoseRecord(payload);
        expect(actual).toEqual(expected);
    });

    test('Undefined returned when processing data fails', () => {
        const payload: FirehoseTransformationEventRecord = {
            recordId: 'jest test',
            data: 'W3MqZSkiolJcWoalWybJSNIJTXcRgjV8fb4BwwLrNzwAgAA',
            approximateArrivalTimestamp: 1510254471091
        };

        const actual = processFirehoseRecord(payload);
        expect(actual).toEqual({
            owner: '',
            logStream: '',
            logGroup: '',
            messageType: '',
            subscriptionFilters: [],
            logEvents: []
        });
    });

    test('Transform single log event to JSON string', () => {
        const logs: CloudWatchLogsDecodedData = {
            messageType: 'DATA_MESSAGE',
            logGroup: 'copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L',
            owner: '263868185958',
            logStream: 'copy-cwl-lambda-invoke-input-1510254365531-LogStream1-18OE12E2F8CE7',
            subscriptionFilters: ['copy-cwl-lambda-invoke-input-1510254365531-SubscriptionFilter-L6TMMNMRQCU8'],
            logEvents: [
                {
                    id: '33679800144697422554415395414062324420037086717597843456',
                    message: '8499846a-88c1-4fb3-bfca-117ec023c5c3',
                    timestamp: 1510254471089
                }
            ]
        };

        const expected = {
            awsAccountId: logs.owner,
            logGroup: logs.logGroup,
            logStream: logs.logStream,
            id: logs.logEvents[0].id,
            message: logs.logEvents[0].message,
            timestamp: logs.logEvents[0].timestamp
        };

        const actual = transformLogEvent(logs);
        expect(actual).toEqual(JSON.stringify(expected).concat('\n'));
    });

    test('Transform record with multiple log events to JSON string', () => {
        const logs: CloudWatchLogsDecodedData = {
            messageType: 'DATA_MESSAGE',
            logGroup: 'copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L',
            owner: '263868185958',
            logStream: 'copy-cwl-lambda-invoke-input-1510254365531-LogStream1-18OE12E2F8CE7',
            subscriptionFilters: ['copy-cwl-lambda-invoke-input-1510254365531-SubscriptionFilter-L6TMMNMRQCU8'],
            logEvents: [
                {
                    id: '33679800144697422554415395414062324420037086717597843456',
                    message: '8499846a-88c1-4fb3-bfca-117ec023c5c3',
                    timestamp: 1510254471089
                },
                {
                    id: '33679800144719723299613926037203860138309735079103823873',
                    message: '5615152f-ae3e-4163-bbd0-c26a241e4ca1',
                    timestamp: 1510254471090
                },
                {
                    id: '33679800144742024044812456660345395856582383440609804290',
                    message: 'bfacbeeb-e5ab-4bdd-b6fc-4f0bebd4fa09',
                    timestamp: 1510254471091
                }
            ]
        };

        let expected = '';
        logs.logEvents.forEach((event) => {
            const record = {
                awsAccountId: logs.owner,
                logGroup: logs.logGroup,
                logStream: logs.logStream,
                id: event.id,
                message: event.message,
                timestamp: event.timestamp
            };
            expected = expected.concat(JSON.stringify(record), '\n');
        });

        const actual = transformLogEvent(logs);
        expect(actual).toEqual(expected);
    });
});
