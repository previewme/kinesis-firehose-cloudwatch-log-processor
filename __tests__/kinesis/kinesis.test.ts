import { ReingestRecord } from '../../src';
import { putRecordsToKinesisStream } from '../../src/kinesis/kinesis';

let mockPutRecords = jest.fn();

jest.mock('aws-sdk', () => {
    return {
        Kinesis: jest.fn(() => {
            return { putRecords: mockPutRecords };
        })
    };
});

describe('Ensure AWS Kinesis interactions are correct ', () => {
    beforeEach(() => {
        jest.clearAllMocks();
        jest.resetModules();
        mockPutRecords = jest.fn(() => {
            return {
                promise: jest.fn(() => {
                    return Promise.resolve({
                        FailedPutCount: 0,
                        Encrypted: false,
                        Records: []
                    });
                })
            };
        });
    });

    test('Can publish single record to kinesis', async () => {
        const records: ReingestRecord[] = [
            {
                Data: Buffer.from('Test'),
                PartitionKey: 'PartitionKey'
            }
        ];
        await putRecordsToKinesisStream('stream', 'region', records, 0, 0);
        expect(mockPutRecords).toBeCalledTimes(1);
    });

    test('Can publish multiple records to kinesis', async () => {
        const records: ReingestRecord[] = [
            {
                Data: Buffer.from('Test1'),
                PartitionKey: 'PartitionKey1'
            },
            {
                Data: Buffer.from('Test2'),
                PartitionKey: 'PartitionKey2'
            }
        ];
        await putRecordsToKinesisStream('stream', 'region', records, 0, 0);
        expect(mockPutRecords).toBeCalledTimes(1);
    });

    test('Failed records are retried successfully', async () => {
        mockPutRecords = jest
            .fn()
            .mockImplementationOnce(
                jest.fn(() => {
                    return {
                        promise: jest.fn(() => {
                            return Promise.resolve({
                                FailedPutCount: 0,
                                Encrypted: false,
                                Records: [
                                    {
                                        SequenceNumber: 'sequenceNumber',
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
                                Records: []
                            });
                        })
                    };
                })
            );

        const records: ReingestRecord[] = [
            {
                Data: Buffer.from('Test'),
                PartitionKey: 'PartitionKey'
            }
        ];
        await putRecordsToKinesisStream('stream', 'region', records, 1, 0);
        expect(mockPutRecords).toBeCalledTimes(2);
    });

    test('Failed records are retried unsuccessfully', async () => {
        mockPutRecords = jest.fn(() => {
            return {
                promise: jest.fn(() => {
                    return Promise.resolve({
                        FailedPutCount: 0,
                        Encrypted: false,
                        Records: [
                            {
                                SequenceNumber: 'sequence1',
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
                PartitionKey: 'PartitionKey'
            }
        ];

        await expect(putRecordsToKinesisStream('stream', 'region', records, 1, 0)).rejects.toThrowError(
            'Could not put records after 1 attempts. Individual error codes: jest test'
        );
        expect(mockPutRecords).toBeCalledTimes(2);
    });

    test('Handles error with request to Kinesis API', async () => {
        mockPutRecords = jest
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
                                Records: []
                            });
                        })
                    };
                })
            );

        const records: ReingestRecord[] = [
            {
                Data: Buffer.from('Test1'),
                PartitionKey: 'PartitionKey1'
            },
            {
                Data: Buffer.from('Test2'),
                PartitionKey: 'PartitionKey2'
            }
        ];
        await putRecordsToKinesisStream('stream', 'region', records, 1, 0);
        expect(mockPutRecords).toBeCalledTimes(2);
    });
});
