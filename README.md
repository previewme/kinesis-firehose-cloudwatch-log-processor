# kinesis-firehose-cloudwatch-log-processor

[![CI Workflow](https://github.com/previewme/kinesis-firehose-cloudwatch-log-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/previewme/lambda-typescript/actions/workflows/ci.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=previewme_kinesis-firehose-cloudwatch-log-processor&metric=coverage)](https://sonarcloud.io/dashboard?id=previewme_kinesis-firehose-cloudwatch-log-processor)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=previewme_kinesis-firehose-cloudwatch-log-processor&metric=vulnerabilities)](https://sonarcloud.io/dashboard?id=previewme_kinesis-firehose-cloudwatch-log-processor)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=previewme_kinesis-firehose-cloudwatch-log-processor&metric=alert_status)](https://sonarcloud.io/dashboard?id=previewme_kinesis-firehose-cloudwatch-log-processor)

Lambda function which transforms, unzips and processes cloudwatch logs.

This function currently does not try to parse the message content. Just adds a new line between every log message so that it can be read my Athena.

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
