import { putRecordsToFirehoseStream } from '../../src/kinesis/firehose';
import { ReingestRecord } from '../../src';

let mockPutRecordBatch = jest.fn();

jest.mock('aws-sdk', () => {
    return {
        Firehose: jest.fn(() => {
            return { putRecordBatch: mockPutRecordBatch };
        })
    };
});

describe('Ensure AWS Firehose interactions are correct ', () => {
    beforeEach(() => {
        jest.clearAllMocks();
        jest.resetModules();
        mockPutRecordBatch = jest.fn(() => {
            return {
                promise: jest.fn(() => {
                    return Promise.resolve({
                        FailedPutCount: 0,
                        Encrypted: false,
                        RequestResponses: []
                    });
                })
            };
        });
    });

    test('Can publish single record to firehose', async () => {
        const records: ReingestRecord[] = [
            {
                Data: Buffer.from('Test'),
                PartitionKey: ''
            }
        ];
        await putRecordsToFirehoseStream('stream', 'region', records, 0, 0);
        expect(mockPutRecordBatch).toBeCalledTimes(1);
    });

    test('Can publish multiple records to firehose', async () => {
        const records: ReingestRecord[] = [
            {
                Data: Buffer.from('Test1'),
                PartitionKey: ''
            },
            {
                Data: Buffer.from('Test2'),
                PartitionKey: ''
            }
        ];
        await putRecordsToFirehoseStream('stream', 'region', records, 0, 0);
        expect(mockPutRecordBatch).toBeCalledTimes(1);
    });

    test('Failed records are retried successfully', async () => {
        mockPutRecordBatch = jest
            .fn()
            .mockImplementationOnce(
                jest.fn(() => {
                    return {
                        promise: jest.fn(() => {
                            return Promise.resolve({
                                FailedPutCount: 0,
                                Encrypted: false,
                                RequestResponses: [
                                    {
                                        RecordId: 'recordId',
                                        ErrorCode: 'jest test',
                                        ErrorMessage: 'message failure test'
                                    }
                                ]
                            });
                        })
                    };
                })
            )
            .mockImplementation(
                jest.fn(() => {
                    return {
                        promise: jest.fn(() => {
                            return Promise.resolve({
                                FailedPutCount: 0,
                                Encrypted: false,
                                RequestResponses: []
                            });
                        })
                    };
                })
            );

        const records: ReingestRecord[] = [
            {
                Data: Buffer.from('Test'),
                PartitionKey: ''
            }
        ];
        await putRecordsToFirehoseStream('stream', 'region', records, 1, 0);
        expect(mockPutRecordBatch).toBeCalledTimes(2);
    });

    test('Failed records are retried unsuccessfully', async () => {
        mockPutRecordBatch = jest.fn(() => {
            return {
                promise: jest.fn(() => {
                    return Promise.resolve({
                        FailedPutCount: 0,
                        Encrypted: false,
                        RequestResponses: [
                            {
                                RecordId: 'recordId',
                                ErrorCode: 'jest test',
                                ErrorMessage: 'message failure test'
                            }
                        ]
                    });
                })
            };
        });

        const records: ReingestRecord[] = [
            {
                Data: Buffer.from('Test'),
                PartitionKey: ''
            }
        ];

        await expect(putRecordsToFirehoseStream('stream', 'region', records, 1, 0)).rejects.toThrowError(
            'Could not put records after 1 attempts. Individual error codes: jest test'
        );
        expect(mockPutRecordBatch).toBeCalledTimes(2);
    });

    test('Handles error with request to Firehose API', async () => {
        mockPutRecordBatch = jest
            .fn()
            .mockImplementationOnce(
                jest.fn(() => {
                    return {
                        promise: jest.fn(() => {
                            return Promise.reject('API Error');
                        })
                    };
                })
            )
            .mockImplementation(
                jest.fn(() => {
                    return {
                        promise: jest.fn(() => {
                            return Promise.resolve({
                                FailedPutCount: 0,
                                Encrypted: false,
                                RequestResponses: []
                            });
                        })
                    };
                })
            );

        const records: ReingestRecord[] = [
            {
                Data: Buffer.from('Test1'),
                PartitionKey: ''
            },
            {
                Data: Buffer.from('Test2'),
                PartitionKey: ''
            }
        ];
        await putRecordsToFirehoseStream('stream', 'region', records, 1, 0);
        expect(mockPutRecordBatch).toBeCalledTimes(2);
    });
});
