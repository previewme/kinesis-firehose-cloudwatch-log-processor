# kinesis-firehose-cloudwatch-log-processor

[![CI Workflow](https://github.com/previewme/kinesis-firehose-cloudwatch-log-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/previewme/lambda-typescript/actions/workflows/ci.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=previewme_kinesis-firehose-cloudwatch-log-processor&metric=coverage)](https://sonarcloud.io/dashboard?id=previewme_kinesis-firehose-cloudwatch-log-processor)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=previewme_kinesis-firehose-cloudwatch-log-processor&metric=vulnerabilities)](https://sonarcloud.io/dashboard?id=previewme_kinesis-firehose-cloudwatch-log-processor)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=previewme_kinesis-firehose-cloudwatch-log-processor&metric=alert_status)](https://sonarcloud.io/dashboard?id=previewme_kinesis-firehose-cloudwatch-log-processor)

Lambda function which transforms, unzips and processes cloudwatch logs.

The function currently does not parse the message content, instead just ensures that AWS Athena can read the logs. The function ensures the account id, log group and log stream is retained for every log message.

## Build

To build the lambda function run the following.

```
npm install
npm run build
```

## Test

To run the tests.

```
npm test
```

## Package

The following will package the lambda function into a zip bundle to allow manual deployment.

```
zip -q -r dist/lambda.zip node_modules dist
```
