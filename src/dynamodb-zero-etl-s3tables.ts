import * as cdk from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3tables from 'aws-cdk-lib/aws-s3tables';
import * as cr from 'aws-cdk-lib/custom-resources';
import { Construct } from 'constructs';

/**
 * Properties for DynamoDbZeroEtlToS3Tables.
 */
export interface DynamoDbZeroEtlToS3TablesProps {
  /**
   * The DynamoDB table to use as the source.
   * Must have an explicit tableName set and PITR enabled.
   */
  readonly table: dynamodb.Table;

  /**
   * Name for the S3 Table Bucket (Iceberg-native).
   */
  readonly tableBucketName: string;

  /**
   * Optional name for the Glue zero-ETL integration.
   * @default 'ddb-to-s3tables'
   */
  readonly integrationName?: string;
}

/**
 * An L3 construct that creates a complete zero-ETL integration
 * from Amazon DynamoDB to Amazon S3 Tables (Apache Iceberg).
 *
 * This construct provisions:
 * - An S3 Table Bucket for Iceberg storage
 * - An IAM role with least-privilege permissions for Glue
 * - A DynamoDB resource policy allowing Glue to export data
 * - A Glue Data Catalog resource policy (via custom resource)
 * - A Glue IntegrationResourceProperty wiring the target role
 * - A Glue CfnIntegration connecting source to target
 */
export class DynamoDbZeroEtlToS3Tables extends Construct {
  /** The S3 Table Bucket created for Iceberg storage. */
  public readonly tableBucket: s3tables.CfnTableBucket;

  /** The IAM role used by Glue to write to the target. */
  public readonly targetRole: iam.Role;

  /** The Glue zero-ETL integration. */
  public readonly integration: glue.CfnIntegration;

  constructor(scope: Construct, id: string, props: DynamoDbZeroEtlToS3TablesProps) {
    super(scope, id);

    const cfnTable = props.table.node.defaultChild as cdk.CfnResource;
    const tableName = (cfnTable as any).tableName as string | undefined;
    if (!tableName || cdk.Token.isUnresolved(tableName)) {
      throw new Error('Table must have an explicit tableName set for zero-ETL integration');
    }

    const pitrSpec = (cfnTable as any).pointInTimeRecoverySpecification;
    if (!pitrSpec || !pitrSpec.pointInTimeRecoveryEnabled) {
      throw new Error('Table must have pointInTimeRecovery enabled for zero-ETL integration');
    }

    const stack = cdk.Stack.of(this);
    const tableArnStr = `arn:aws:dynamodb:${stack.region}:${stack.account}:table/${tableName}`;
    const bucketArnStr = `arn:aws:s3tables:${stack.region}:${stack.account}:bucket/${props.tableBucketName}`;
    const s3TablesCatalogArn = `arn:aws:glue:${stack.region}:${stack.account}:catalog/s3tablescatalog/${props.tableBucketName}`;

    // S3 Table Bucket (Iceberg-native)
    this.tableBucket = new s3tables.CfnTableBucket(this, 'TableBucket', {
      tableBucketName: props.tableBucketName,
    });

    // Target IAM role
    this.targetRole = new iam.Role(this, 'TargetRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });

    this.targetRole.addToPolicy(new iam.PolicyStatement({
      actions: ['s3tables:GetTableBucket'],
      resources: [bucketArnStr],
    }));

    this.targetRole.addToPolicy(new iam.PolicyStatement({
      actions: ['s3tables:GetNamespace', 's3tables:ListNamespaces', 's3tables:CreateNamespace'],
      resources: [bucketArnStr, `${bucketArnStr}/namespace/*`],
    }));

    this.targetRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        's3tables:GetTable', 's3tables:CreateTable', 's3tables:ListTables',
        's3tables:GetTableMetadataLocation', 's3tables:UpdateTableMetadataLocation',
        's3tables:GetTableData', 's3tables:PutTableData',
      ],
      resources: [bucketArnStr, `${bucketArnStr}/namespace/*`, `${bucketArnStr}/table/*`],
    }));

    this.targetRole.addToPolicy(new iam.PolicyStatement({
      actions: ['glue:GetDatabase'],
      resources: [
        `arn:aws:glue:${stack.region}:${stack.account}:catalog`,
        `arn:aws:glue:${stack.region}:${stack.account}:database/*`,
      ],
    }));

    this.targetRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'glue:CreateTable', 'glue:GetTable', 'glue:GetTables',
        'glue:DeleteTable', 'glue:UpdateTable',
        'glue:GetTableVersion', 'glue:GetTableVersions', 'glue:GetResourcePolicy',
      ],
      resources: [
        `arn:aws:glue:${stack.region}:${stack.account}:catalog`,
        `arn:aws:glue:${stack.region}:${stack.account}:database/*`,
        `arn:aws:glue:${stack.region}:${stack.account}:table/*/*`,
      ],
    }));

    this.targetRole.addToPolicy(new iam.PolicyStatement({
      actions: ['cloudwatch:PutMetricData'],
      resources: ['*'],
      conditions: { StringEquals: { 'cloudwatch:namespace': 'AWS/Glue/ZeroETL' } },
    }));

    this.targetRole.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
      resources: ['*'],
    }));

    // DynamoDB resource policy — allow Glue to export
    props.table.addToResourcePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      principals: [new iam.ServicePrincipal('glue.amazonaws.com')],
      actions: [
        'dynamodb:ExportTableToPointInTime',
        'dynamodb:DescribeTable',
        'dynamodb:DescribeExport',
      ],
      resources: ['*'],
      conditions: {
        StringEquals: { 'aws:SourceAccount': stack.account },
        ArnLike: { 'aws:SourceArn': `arn:aws:glue:${stack.region}:${stack.account}:integration:*` },
      },
    }));

    const catalogArn = `arn:aws:glue:${stack.region}:${stack.account}:catalog`;
    const databaseArn = `arn:aws:glue:${stack.region}:${stack.account}:database/*`;

    // Glue Catalog resource policy (no CFN support — use custom resource)
    const catalogPolicy = new cr.AwsCustomResource(this, 'CatalogPolicy', {
      onCreate: {
        service: 'Glue',
        action: 'putResourcePolicy',
        parameters: {
          PolicyInJson: JSON.stringify({
            Version: '2012-10-17',
            Statement: [
              {
                Effect: 'Allow',
                Principal: { AWS: `arn:aws:iam::${stack.account}:root` },
                Action: 'glue:CreateInboundIntegration',
                Resource: [catalogArn, databaseArn],
                Condition: {
                  StringLike: { 'aws:SourceArn': tableArnStr },
                },
              },
              {
                Effect: 'Allow',
                Principal: { Service: 'glue.amazonaws.com' },
                Action: 'glue:AuthorizeInboundIntegration',
                Resource: [catalogArn, databaseArn],
                Condition: {
                  StringEquals: { 'aws:SourceArn': tableArnStr },
                },
              },
              {
                Effect: 'Allow',
                Principal: { AWS: `arn:aws:iam::${stack.account}:root` },
                Action: 'glue:CreateInboundIntegration',
                Resource: s3TablesCatalogArn,
              },
              {
                Effect: 'Allow',
                Principal: { Service: 'glue.amazonaws.com' },
                Action: 'glue:AuthorizeInboundIntegration',
                Resource: s3TablesCatalogArn,
              },
            ],
          }),
          EnableHybrid: 'TRUE',
        },
        physicalResourceId: cr.PhysicalResourceId.of('CatalogPolicy'),
      },
      onDelete: {
        service: 'Glue',
        action: 'deleteResourcePolicy',
        parameters: {},
      },
      policy: cr.AwsCustomResourcePolicy.fromStatements([
        new iam.PolicyStatement({
          actions: ['glue:PutResourcePolicy', 'glue:DeleteResourcePolicy'],
          resources: [`arn:aws:glue:${stack.region}:${stack.account}:catalog`],
        }),
      ]),
    });

    // Wire target role to integration target
    const targetResourceProperty = new glue.CfnIntegrationResourceProperty(this, 'TargetResourceProperty', {
      resourceArn: s3TablesCatalogArn,
      targetProcessingProperties: {
        roleArn: this.targetRole.roleArn,
      },
    });
    targetResourceProperty.addDependency(this.tableBucket);

    // Glue Zero-ETL Integration
    this.integration = new glue.CfnIntegration(this, 'Integration', {
      integrationName: props.integrationName ?? 'ddb-to-s3tables',
      sourceArn: tableArnStr,
      targetArn: s3TablesCatalogArn,
    });

    // Dependency chain
    this.integration.addDependency(this.tableBucket);
    this.integration.node.addDependency(props.table);
    this.integration.node.addDependency(catalogPolicy);
    this.integration.node.addDependency(targetResourceProperty);
  }
}
