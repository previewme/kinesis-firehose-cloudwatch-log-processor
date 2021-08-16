import { CloudWatchLogsDecodedData, FirehoseTransformationEventRecord, FirehoseTransformationResultRecord } from 'aws-lambda';
import { calculateRecordsToReingest, createReingestRecord, handler, processRecords, ReingestRecords } from '../src';
import { default as cwLogEvent } from './resources/cw-log-event.json';
import { default as cwLogKinesisEvent } from './resources/cw-log-kinesis-event.json';
import { putRecordsToFirehoseStream } from '../src/kinesis/firehose';
import { putRecordsToKinesisStream } from '../src/kinesis/kinesis';
import { gzipSync } from 'zlib';

jest.mock('../src/kinesis/firehose');
jest.mock('../src/kinesis/kinesis');

describe('Ensure processing firehose records occurs correctly', function () {
    const kinesisRecord: FirehoseTransformationEventRecord = {
        recordId: 'jest test',
        data: 'H4sIAAAAAAAAAJWRTWsbMRCG/8ueLZjRjL5yc9NNLnZDapemlFAkrTYstb3Lep0Qgv97x00KgTSHnAQzmkeP3nmqtmW/j3dl/TiU6qz6PF/Pfy3r1Wp+WVezqn/YlVHK2pK3Hr0Jxkt5099djv1hkE7uh0eVHzZqE7epiarb3fe/ixzDYVJoELRhssYQqsXLlEJ3jd8//biy4QYWz7jVNJa4/TDveQwV+qsada0v/HnthLg/pH0eu2Hq+t1Ft5nKuK/Ofn4EvnpDUAu7Xi6/LL9en3/z1e1f7fq+7KYT+qnqGrEnsi54AGS2wbHWxjCjoWAYGawmzawByIG3Dp0JzjOxsaI8dbKJKW4l1BcTdgg+zP5tSPCeQ/Bso/I+o+I2kUptjgrRlQyasslUHWdvZRwGJ4+HYJGCtiKgQTYKSJ4gODLgAkpFk3f0rkyA1zLGSsvoVsVCRTFakUkNqKxt1IyFc8T/y0gEmoHZo5a/W9HhU0TeWHMyIJaoQC6zDvC+DL6WSW3MqZSkiolJcWoalWybJSNIJTXcRgjV8fb4BwwLrNzwAgAA',
        approximateArrivalTimestamp: 1510254471091,
        kinesisRecordMetadata: {
            partitionKey: 'partitionKey',
            shardId: 'shardId',
            approximateArrivalTimestamp: 1510254471091,
            sequenceNumber: 'sequenceNumber',
            subsequenceNumber: 'subsequenceNumber'
        }
    };

    const firehoseRecord = {
        recordId: 'jest test',
        data: 'H4sIAAAAAAAAAJWRTWsbMRCG/8ueLZjRjL5yc9NNLnZDapemlFAkrTYstb3Lep0Qgv97x00KgTSHnAQzmkeP3nmqtmW/j3dl/TiU6qz6PF/Pfy3r1Wp+WVezqn/YlVHK2pK3Hr0Jxkt5099djv1hkE7uh0eVHzZqE7epiarb3fe/ixzDYVJoELRhssYQqsXLlEJ3jd8//biy4QYWz7jVNJa4/TDveQwV+qsada0v/HnthLg/pH0eu2Hq+t1Ft5nKuK/Ofn4EvnpDUAu7Xi6/LL9en3/z1e1f7fq+7KYT+qnqGrEnsi54AGS2wbHWxjCjoWAYGawmzawByIG3Dp0JzjOxsaI8dbKJKW4l1BcTdgg+zP5tSPCeQ/Bso/I+o+I2kUptjgrRlQyasslUHWdvZRwGJ4+HYJGCtiKgQTYKSJ4gODLgAkpFk3f0rkyA1zLGSsvoVsVCRTFakUkNqKxt1IyFc8T/y0gEmoHZo5a/W9HhU0TeWHMyIJaoQC6zDvC+DL6WSW3MqZSkiolJcWoalWybJSNIJTXcRgjV8fb4BwwLrNzwAgAA',
        approximateArrivalTimestamp: 1510254471091
    };

    test('Ensure empty cloudwatch log data is failed', () => {
        const data = {
            owner: '',
            logStream: '',
            logGroup: '',
            messageType: '',
            subscriptionFilters: [],
            logEvents: []
        };

        const expected = {
            recordId: 'test001',
            result: 'ProcessingFailed',
            data: ''
        };

        const actual = processRecords(expected.recordId, data);
        expect(actual).toEqual(expected);
    });

    test('Ensure unknown message type processing is failed', () => {
        const data = {
            owner: '',
            logStream: '',
            logGroup: '',
            messageType: 'SOMETHING_WE_DONT_UNDERSTAND',
            subscriptionFilters: [],
            logEvents: []
        };

        const expected = {
            recordId: 'test002',
            result: 'ProcessingFailed',
            data: ''
        };

        const actual = processRecords(expected.recordId, data);
        expect(actual).toEqual(expected);
    });

    test('Ensure control messages are dropped', () => {
        const data = {
            owner: '012345678901',
            logStream: '',
            logGroup: '',
            messageType: 'CONTROL_MESSAGE',
            subscriptionFilters: [],
            logEvents: []
        };

        const expected = {
            recordId: 'test003',
            result: 'Dropped',
            data: ''
        };

        const actual = processRecords(expected.recordId, data);
        expect(actual).toEqual(expected);
    });

    test('Ensure data messages are processed with result of OK', () => {
        const data: CloudWatchLogsDecodedData = {
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

        const event = {
            awsAccountId: data.owner,
            logGroup: data.logGroup,
            logStream: data.logStream,
            id: data.logEvents[0].id,
            message: data.logEvents[0].message,
            timestamp: data.logEvents[0].timestamp
        };

        const expected = {
            recordId: 'test004',
            result: 'Ok',
            data: Buffer.from(JSON.stringify(event).concat('\n')).toString('base64')
        };

        const actual = processRecords(expected.recordId, data);
        expect(actual).toEqual(expected);
    });

    test('Ensure unknown message type processing is failed', () => {
        const data = {
            owner: '',
            logStream: '',
            logGroup: '',
            messageType: 'SOMETHING_WE_DONT_UNDERSTAND',
            subscriptionFilters: [],
            logEvents: []
        };

        const expected = {
            recordId: 'test002',
            result: 'ProcessingFailed',
            data: ''
        };

        const actual = processRecords(expected.recordId, data);
        expect(actual).toEqual(expected);
    });

    test('Ensure messages over the maximum size are failed', () => {
        const data: CloudWatchLogsDecodedData = {
            messageType: 'DATA_MESSAGE',
            logGroup: 'copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L',
            owner: '263868185958',
            logStream: 'copy-cwl-lambda-invoke-input-1510254365531-LogStream1-18OE12E2F8CE7',
            subscriptionFilters: ['copy-cwl-lambda-invoke-input-1510254365531-SubscriptionFilter-L6TMMNMRQCU8'],
            logEvents: [
                {
                    id: '33679800144697422554415395414062324420037086717597843456',
                    message: 'a'.repeat(5999705),
                    timestamp: 1510254471089
                }
            ]
        };

        const expected = {
            recordId: 'test002',
            result: 'ProcessingFailed',
            data: ''
        };

        const actual = processRecords(expected.recordId, data);
        expect(actual).toEqual(expected);
    });

    test('Able to create reingest record when source is a stream', () => {
        const expected = {
            Data: Buffer.from(kinesisRecord.data, 'base64'),
            PartitionKey: kinesisRecord.kinesisRecordMetadata?.partitionKey
        };
        const actual = createReingestRecord(kinesisRecord, true);
        expect(actual).toEqual(expected);
    });

    test('Able to create reingest record when source is a delivery stream', () => {
        const expected = {
            Data: Buffer.from(kinesisRecord.data, 'base64'),
            PartitionKey: ''
        };
        const actual = createReingestRecord(firehoseRecord, false);
        expect(actual).toEqual(expected);
    });

    test('Dropped and Processed Failed records do not get reingested ', () => {
        const resultRecords: FirehoseTransformationResultRecord[] = [];
        for (let i = 0; i < 10; i++) {
            resultRecords.push({
                recordId: 'failedRecord' + i,
                data: '',
                result: 'ProcessingFailed'
            });
            resultRecords.push({
                recordId: 'droppedRecord' + i,
                data: '',
                result: 'Dropped'
            });
        }
        const processResult = calculateRecordsToReingest(resultRecords, {});
        expect(processResult.responseRecords).toEqual(resultRecords);
        expect(processResult.reingestBatches).toEqual([]);
        expect(processResult.reingestRecordCount).toEqual(0);
    });

    test('No records to reingest when within size limits', () => {
        const resultRecords: FirehoseTransformationResultRecord[] = [];
        for (let i = 0; i < 10; i++) {
            resultRecords.push({
                recordId: 'record' + i,
                data: '' + i,
                result: 'Ok'
            });
        }

        const processResult = calculateRecordsToReingest(resultRecords, {});
        expect(processResult.responseRecords).toEqual(resultRecords);
        expect(processResult.reingestBatches).toEqual([]);
        expect(processResult.reingestRecordCount).toEqual(0);
    });

    test('Check records when buffer size limits are in reingest list', () => {
        const resultRecords: FirehoseTransformationResultRecord[] = [
            {
                recordId: 'record',
                data: 'a'.repeat(5999999),
                result: 'Ok'
            }
        ];

        const reingestRecords: ReingestRecords = {
            record: {
                Data: Buffer.from(resultRecords[0].data),
                PartitionKey: 'partitionKey'
            }
        };

        const actual = calculateRecordsToReingest(resultRecords, reingestRecords);
        expect(actual.responseRecords).toEqual([{ recordId: 'record', data: '', result: 'Dropped' }]);
        expect(actual.reingestBatches).toEqual([[reingestRecords['record']]]);
        expect(actual.reingestRecordCount).toEqual(1);
    });

    test('Reingestion should have 500 records per batch', () => {
        const resultRecords: FirehoseTransformationResultRecord[] = [
            {
                recordId: 'record',
                data: 'a'.repeat(5999999),
                result: 'Ok'
            }
        ];

        for (let i = 1; i < 501; i++) {
            resultRecords.push({
                recordId: 'record',
                data: 'b',
                result: 'Ok'
            });
        }

        const reingestRecords: ReingestRecords = {
            record: {
                Data: Buffer.from(resultRecords[0].data),
                PartitionKey: 'partitionKey'
            }
        };

        const actual = calculateRecordsToReingest(resultRecords, reingestRecords);
        expect(actual.reingestBatches.length).toEqual(2);
        expect(actual.reingestRecordCount).toEqual(501);
    });

    test('Processes Cloudwatch Log event', async () => {
        const response = await handler(cwLogEvent);
        expect(response.records.length).toEqual(4);
        expect(response.records[0].result).toEqual('Dropped');
        expect(response.records[1].result).toEqual('Ok');
        expect(response.records[2].result).toEqual('Ok');
        expect(response.records[3].result).toEqual('Ok');
    });

    test('Processes Cloudwatch Log event with reingestion to Firehose', async () => {
        const mockPutRecordsBatch = putRecordsToFirehoseStream as jest.MockedFunction<typeof putRecordsToFirehoseStream>;
        mockPutRecordsBatch.mockReturnValue(Promise.resolve());

        const bigMessage = {
            messageType: 'DATA_MESSAGE',
            owner: '263868185958',
            logGroup: 'copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L',
            logStream: 'copy-cwl-lambda-invoke-input-1510254365531-LogStream1-18OE12E2F8CE7',
            subscriptionFilters: ['copy-cwl-lambda-invoke-input-1510254365531-SubscriptionFilter-L6TMMNMRQCU8'],
            logEvents: [
                {
                    id: '33679800144697422554415395414062324420037086717597843456',
                    timestamp: 1510254471089,
                    message: 'a'.repeat(1998000)
                },
                {
                    id: '33679800144719723299613926037203860138309735079103823873',
                    timestamp: 1510254471090,
                    message: 'b'.repeat(1500000)
                },
                {
                    id: '33679800144742024044812456660345395856582383440609804290',
                    timestamp: 1510254471091,
                    message: 'c'.repeat(1000000)
                }
            ]
        };

        cwLogEvent.records[3].data = gzipSync(Buffer.from(JSON.stringify(bigMessage))).toString('base64');
        const response = await handler(cwLogEvent);
        expect(response.records.length).toEqual(4);
        expect(response.records[0].result).toEqual('Dropped');
        expect(response.records[1].result).toEqual('Ok');
        expect(response.records[2].result).toEqual('Ok');
        expect(response.records[3].result).toEqual('Dropped');

        expect(mockPutRecordsBatch).toBeCalledTimes(1);
    });

    test('Processes Cloudwatch Log event with reingestion to Kinesis', async () => {
        const mockPutRecords = putRecordsToKinesisStream as jest.MockedFunction<typeof putRecordsToKinesisStream>;
        mockPutRecords.mockReturnValue(Promise.resolve());

        const bigMessage = {
            messageType: 'DATA_MESSAGE',
            owner: '263868185958',
            logGroup: 'copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L',
            logStream: 'copy-cwl-lambda-invoke-input-1510254365531-LogStream1-18OE12E2F8CE7',
            subscriptionFilters: ['copy-cwl-lambda-invoke-input-1510254365531-SubscriptionFilter-L6TMMNMRQCU8'],
            logEvents: [
                {
                    id: '33679800144697422554415395414062324420037086717597843456',
                    timestamp: 1510254471089,
                    message: 'a'.repeat(1998000)
                },
                {
                    id: '33679800144719723299613926037203860138309735079103823873',
                    timestamp: 1510254471090,
                    message: 'b'.repeat(1500000)
                },
                {
                    id: '33679800144742024044812456660345395856582383440609804290',
                    timestamp: 1510254471091,
                    message: 'c'.repeat(1000000)
                }
            ]
        };

        cwLogKinesisEvent.records[3].data = gzipSync(Buffer.from(JSON.stringify(bigMessage))).toString('base64');
        const response = await handler(cwLogKinesisEvent);
        expect(response.records.length).toEqual(4);
        expect(response.records[0].result).toEqual('Dropped');
        expect(response.records[1].result).toEqual('Ok');
        expect(response.records[2].result).toEqual('Ok');
        expect(response.records[3].result).toEqual('Dropped');

        expect(mockPutRecords).toBeCalledTimes(1);
    });
});
