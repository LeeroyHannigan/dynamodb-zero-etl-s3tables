import * as cdk from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { Template, Match } from 'aws-cdk-lib/assertions';
import { DynamoDbZeroEtlToS3Tables } from '../src';

function createStack() {
  const app = new cdk.App();
  const stack = new cdk.Stack(app, 'TestStack', {
    env: { account: '123456789012', region: 'us-east-1' },
  });
  return stack;
}

function createTable(stack: cdk.Stack, name = 'TestTable') {
  return new dynamodb.Table(stack, 'Table', {
    tableName: name,
    partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
    sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
    billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
  });
}

describe('DynamoDbZeroEtlToS3Tables', () => {
  test('creates all expected resources', () => {
    const stack = createStack();
    const table = createTable(stack);

    new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'test-bucket',
    });

    const template = Template.fromStack(stack);
    template.resourceCountIs('AWS::S3Tables::TableBucket', 1);
    template.resourceCountIs('AWS::IAM::Role', 2); // target role + Lambda execution role
    template.resourceCountIs('AWS::Glue::Integration', 1);
    template.resourceCountIs('AWS::Glue::IntegrationResourceProperty', 1);
  });

  test('creates S3 Table Bucket with correct name', () => {
    const stack = createStack();
    const table = createTable(stack);

    new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-iceberg-bucket',
    });

    const template = Template.fromStack(stack);
    template.hasResourceProperties('AWS::S3Tables::TableBucket', {
      TableBucketName: 'my-iceberg-bucket',
    });
  });

  test('creates integration with correct source and target', () => {
    const stack = createStack();
    const table = createTable(stack);

    new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-bucket',
      integrationName: 'my-integration',
    });

    const template = Template.fromStack(stack);
    template.hasResourceProperties('AWS::Glue::Integration', {
      IntegrationName: 'my-integration',
      SourceArn: {
        'Fn::Join': ['', ['arn:', { Ref: 'AWS::Partition' }, ':dynamodb:us-east-1:123456789012:table/TestTable']],
      },
      TargetArn: {
        'Fn::Join': ['', ['arn:', { Ref: 'AWS::Partition' }, ':glue:us-east-1:123456789012:catalog/s3tablescatalog/my-bucket']],
      },
    });
  });

  test('uses default integration name when not specified', () => {
    const stack = createStack();
    const table = createTable(stack);

    new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-bucket',
    });

    const template = Template.fromStack(stack);
    template.hasResourceProperties('AWS::Glue::Integration', {
      IntegrationName: 'ddb-to-s3tables',
    });
  });

  test('target role has s3tables permissions scoped to bucket', () => {
    const stack = createStack();
    const table = createTable(stack);

    new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-bucket',
    });

    const template = Template.fromStack(stack);
    template.hasResourceProperties('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: Match.arrayWith([
          Match.objectLike({
            Action: 's3tables:GetTableBucket',
            Resource: {
              'Fn::Join': ['', ['arn:', { Ref: 'AWS::Partition' }, ':s3tables:us-east-1:123456789012:bucket/my-bucket']],
            },
          }),
        ]),
      },
    });
  });

  test('target role has glue catalog permissions', () => {
    const stack = createStack();
    const table = createTable(stack);

    new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-bucket',
    });

    const template = Template.fromStack(stack);
    template.hasResourceProperties('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: Match.arrayWith([
          Match.objectLike({
            Action: 'glue:GetDatabase',
          }),
        ]),
      },
    });
  });

  test('sets DynamoDB resource policy for Glue export', () => {
    const stack = createStack();
    const table = createTable(stack);

    new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-bucket',
    });

    const template = Template.fromStack(stack);
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      ResourcePolicy: {
        PolicyDocument: Match.objectLike({
          Statement: Match.arrayWith([
            Match.objectLike({
              Action: Match.arrayWith(['dynamodb:ExportTableToPointInTime']),
              Principal: { Service: 'glue.amazonaws.com' },
            }),
          ]),
        }),
      },
    });
  });

  test('creates custom resource for catalog policy', () => {
    const stack = createStack();
    const table = createTable(stack);

    new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-bucket',
    });

    const template = Template.fromStack(stack);
    template.resourceCountIs('AWS::Lambda::Function', 1);
    template.hasResourceProperties('AWS::CloudFormation::CustomResource', {
      Statements: {
        'Fn::Join': ['', Match.arrayWith([Match.stringLikeRegexp('CreateInbound')])],
      },
    });
  });

  test('throws if table has no explicit tableName', () => {
    const stack = createStack();
    const table = new dynamodb.Table(stack, 'Table', {
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
    });

    expect(() => {
      new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
        table,
        tableBucketName: 'my-bucket',
      });
    }).toThrow('Table must have an explicit tableName set for zero-ETL integration');
  });

  test('throws if PITR is not enabled', () => {
    const stack = createStack();
    const table = new dynamodb.Table(stack, 'Table', {
      tableName: 'MyTable',
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
    });

    expect(() => {
      new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
        table,
        tableBucketName: 'my-bucket',
      });
    }).toThrow('Table must have pointInTimeRecovery enabled for zero-ETL integration');
  });

  test('exposes public properties', () => {
    const stack = createStack();
    const table = createTable(stack);

    const zeroEtl = new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-bucket',
    });

    expect(zeroEtl.tableBucket).toBeDefined();
    expect(zeroEtl.targetRole).toBeDefined();
    expect(zeroEtl.integration).toBeDefined();
  });

  test('allows adding custom policy to target role', () => {
    const stack = createStack();
    const table = createTable(stack);

    const zeroEtl = new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-bucket',
    });

    zeroEtl.targetRole.addToPolicy(new cdk.aws_iam.PolicyStatement({
      actions: ['s3:GetObject'],
      resources: ['arn:aws:s3:::custom-bucket/*'],
    }));

    const template = Template.fromStack(stack);
    template.hasResourceProperties('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: Match.arrayWith([
          Match.objectLike({
            Action: 's3:GetObject',
            Resource: 'arn:aws:s3:::custom-bucket/*',
          }),
        ]),
      },
    });
  });

  test('allows configuring table bucket unreferenced file removal', () => {
    const stack = createStack();
    const table = createTable(stack);

    const zeroEtl = new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-bucket',
    });

    zeroEtl.tableBucket.unreferencedFileRemoval = {
      status: 'Enabled',
      unreferencedDays: 10,
      noncurrentDays: 30,
    };

    const template = Template.fromStack(stack);
    template.hasResourceProperties('AWS::S3Tables::TableBucket', {
      UnreferencedFileRemoval: {
        Status: 'Enabled',
        UnreferencedDays: 10,
        NoncurrentDays: 30,
      },
    });
  });

  test('allows adding tags to integration', () => {
    const stack = createStack();
    const table = createTable(stack);

    const zeroEtl = new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-bucket',
    });

    zeroEtl.integration.tags = [{ key: 'Environment', value: 'production' }];

    const template = Template.fromStack(stack);
    template.hasResourceProperties('AWS::Glue::Integration', {
      Tags: Match.arrayWith([
        { Key: 'Environment', Value: 'production' },
      ]),
    });
  });

  test('target role glue permissions are scoped to the s3tables federated catalog', () => {
    const stack = createStack();
    const table = createTable(stack);

    new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-bucket',
    });

    const rendered = JSON.stringify(Template.fromStack(stack).toJSON());
    expect(rendered).toContain(':database/s3tablescatalog/my-bucket/*');
    expect(rendered).toContain(':table/s3tablescatalog/my-bucket/*/*');
    expect(rendered).not.toContain(':table/*/*');
  });

  test('s3tables catalog policy statements carry source conditions', () => {
    const stack = createStack();
    const table = createTable(stack);

    new DynamoDbZeroEtlToS3Tables(stack, 'ZeroEtl', {
      table,
      tableBucketName: 'my-bucket',
    });

    const rendered = JSON.stringify(Template.fromStack(stack).toJSON());
    const s3TablesConditioned = rendered.match(/CreateInbound_S3Tables[\s\S]{0,700}?aws:SourceArn/);
    expect(s3TablesConditioned).not.toBeNull();
    const authConditioned = rendered.match(/AuthInbound_S3Tables[\s\S]{0,700}?aws:SourceArn/);
    expect(authConditioned).not.toBeNull();
  });
});
