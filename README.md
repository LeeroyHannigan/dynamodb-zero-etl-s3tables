# dynamodb-zero-etl-s3tables

[![npm version](https://img.shields.io/npm/v/dynamodb-zero-etl-s3tables.svg)](https://www.npmjs.com/package/dynamodb-zero-etl-s3tables)
[![PyPI version](https://img.shields.io/pypi/v/dynamodb-zero-etl-s3tables.svg)](https://pypi.org/project/dynamodb-zero-etl-s3tables/)
[![NuGet version](https://img.shields.io/nuget/v/LeeroyHannigan.CDK.DynamoDbZeroEtlS3Tables.svg)](https://www.nuget.org/packages/LeeroyHannigan.CDK.DynamoDbZeroEtlS3Tables/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![jsii](https://img.shields.io/badge/jsii-compatible-brightgreen.svg)](https://github.com/aws/jsii)
[![stability: experimental](https://img.shields.io/badge/stability-experimental-orange.svg)](https://www.npmjs.com/package/dynamodb-zero-etl-s3tables)

An AWS CDK L3 construct that wires up a complete **zero-ETL integration** from **Amazon DynamoDB** to **Amazon S3 Tables** (Apache Iceberg) — in a single line of code.

> **Zero-ETL** eliminates the need to build and maintain ETL pipelines. Data flows automatically from your DynamoDB table into Iceberg tables on S3, ready for analytics with Athena, Redshift, EMR, and more.

## Why this construct?

Setting up DynamoDB zero-ETL to S3 Tables manually requires **7+ resources** across DynamoDB, S3 Tables, IAM, Glue, and custom resources — each with specific permissions, dependencies, and ordering constraints. One misconfigured policy and the integration silently fails.

This construct handles all of that for you:

```
┌──────────────┐         ┌──────────────────┐         ┌─────────────────┐
│              │         │                  │         │                 │
│   DynamoDB   │────────▶│  AWS Glue        │────────▶│  S3 Tables      │
│   Table      │  zero   │  Integration     │  write  │  (Iceberg)      │
│              │  ETL    │                  │         │                 │
└──────────────┘         └──────────────────┘         └─────────────────┘
       │                        │                            │
       ▼                        ▼                            ▼
  Resource Policy          Catalog Policy              Table Bucket
  (Glue export)            (Custom Resource)           IAM Target Role
```

**What gets created:**

| Resource | Purpose |
|----------|---------|
| `AWS::S3Tables::TableBucket` | Iceberg-native storage for your analytics data |
| `AWS::IAM::Role` | Least-privilege role for Glue to write to S3 Tables and catalog |
| `AWS::Glue::Integration` | The zero-ETL integration connecting source to target |
| `AWS::Glue::IntegrationResourceProperty` | Wires the target IAM role to the integration |
| `Custom::AWS` (AwsCustomResource) | Sets the Glue Data Catalog resource policy (no CloudFormation support) |
| DynamoDB Resource Policy | Allows Glue to export and describe the source table |

## Installation

**TypeScript/JavaScript:**

```bash
npm install dynamodb-zero-etl-s3tables
```

**Python:**

```bash
pip install dynamodb-zero-etl-s3tables
```

**Java (Maven):**

```xml
<dependency>
    <groupId>io.github.leeroyhannigan</groupId>
    <artifactId>dynamodb-zero-etl-s3tables</artifactId>
</dependency>
```

**.NET:**

```bash
dotnet add package LeeroyHannigan.CDK.DynamoDbZeroEtlS3Tables
```

## Quick Start

```ts
import { DynamoDbZeroEtlToS3Tables } from 'dynamodb-zero-etl-s3tables';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

const table = new dynamodb.Table(this, 'Table', {
  tableName: 'Orders',
  partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
  sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
  billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
  pointInTimeRecovery: true,
});

new DynamoDbZeroEtlToS3Tables(this, 'ZeroEtl', {
  table,
  tableBucketName: 'orders-iceberg-bucket',
});
```

That's it. Your DynamoDB data will automatically replicate to Iceberg tables on S3.

## Props

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `table` | `dynamodb.Table` | Yes | — | DynamoDB table with an explicit `tableName` and PITR enabled |
| `tableBucketName` | `string` | Yes | — | Name for the S3 Table Bucket |
| `integrationName` | `string` | No | `'ddb-to-s3tables'` | Name for the Glue zero-ETL integration |

## Exposed Properties

All key resources are exposed as public properties for extension:

| Property | Type | Description |
|----------|------|-------------|
| `tableBucket` | `s3tables.CfnTableBucket` | The S3 Table Bucket for Iceberg storage |
| `targetRole` | `iam.Role` | The IAM role Glue uses to write to the target |
| `integration` | `glue.CfnIntegration` | The Glue zero-ETL integration |

## Customization Examples

### Add custom permissions to the target role

```ts
const zeroEtl = new DynamoDbZeroEtlToS3Tables(this, 'ZeroEtl', {
  table,
  tableBucketName: 'my-bucket',
});

zeroEtl.targetRole.addToPolicy(new iam.PolicyStatement({
  actions: ['s3:GetObject'],
  resources: ['arn:aws:s3:::my-other-bucket/*'],
}));
```

### Configure Iceberg file maintenance

```ts
zeroEtl.tableBucket.unreferencedFileRemoval = {
  status: 'Enabled',
  unreferencedDays: 10,
  noncurrentDays: 30,
};
```

### Tag the integration

```ts
zeroEtl.integration.tags = [
  { key: 'Environment', value: 'production' },
  { key: 'Team', value: 'analytics' },
];
```

## Prerequisites

Your DynamoDB table **must** have:

1. **An explicit `tableName`** — auto-generated names (CloudFormation tokens) are not supported. The construct validates this at synth time.
2. **Point-in-time recovery (PITR) enabled** — required by the zero-ETL integration for data export. The construct validates this at synth time.

If either requirement is not met, the construct throws a descriptive error during synthesis.

## How It Works

1. **S3 Table Bucket** is created as the Iceberg-native target for your data
2. **IAM Role** is created with least-privilege permissions for S3 Tables, Glue Catalog, CloudWatch, and Logs
3. **DynamoDB Resource Policy** is set on your table, allowing the Glue service to export data
4. **Glue Catalog Resource Policy** is applied via a custom resource (CloudFormation doesn't support this natively)
5. **Integration Resource Property** wires the IAM role to the target catalog
6. **Glue Integration** is created, connecting your DynamoDB table to the S3 Tables catalog

All resources are created with correct dependency ordering to ensure a successful single-deploy experience.

## Querying Your Data

Once the integration is active, your DynamoDB data is available as Iceberg tables. Query with Amazon Athena:

```sql
SELECT * FROM "s3tablescatalog/my-bucket"."namespace"."table_name" LIMIT 10;
```

## Security

- All IAM permissions follow **least-privilege** principles
- S3 Tables permissions are scoped to the specific bucket and sub-resources
- Glue catalog permissions are scoped to the account's catalog and databases
- DynamoDB resource policy uses `aws:SourceAccount` and `aws:SourceArn` conditions
- CloudWatch metrics are conditioned on the `AWS/Glue/ZeroETL` namespace

## Contributing

Contributions, issues, and feature requests are welcome!

- [GitHub Repository](https://github.com/LeeroyHannigan/dynamodb-zero-etl-s3tables)
- [Issue Tracker](https://github.com/LeeroyHannigan/dynamodb-zero-etl-s3tables/issues)

## License

This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).

## Author

**Lee Hannigan** — [GitHub](https://github.com/LeeroyHannigan)
