# Glue to Redshift CDK stack

This project deploys a production-oriented AWS Glue PySpark pipeline that:

1. reads an existing Lake Formation-managed Glue Catalog table;
2. transforms the data;
3. uses an S3 temporary directory;
4. loads an existing Amazon Redshift table through a private JDBC connection.

## Resources created

- Glue job IAM role
- Glue PySpark job
- Glue JDBC connection
- Glue security group
- S3 script bucket
- S3 temporary/staging bucket
- Lake Formation `DESCRIBE` and `SELECT` grants
- CloudWatch alarm and SNS topic
- optional EventBridge schedule

## Resources expected to exist

- VPC and private subnet
- Redshift cluster or serverless endpoint
- Redshift security group
- Redshift credentials secret
- source Glue Catalog database/table
- source S3 location registered with Lake Formation

## Prerequisites

The deploying CloudFormation execution role must be allowed to:

- create the resources listed above;
- grant Lake Formation permissions;
- modify the imported Redshift security group;
- pass the Glue job role to AWS Glue.

The identity performing the Lake Formation grant must be a Lake Formation data
lake administrator or otherwise have grantable permissions on the source
database/table.

The Redshift secret must contain:

```json
{
  "username": "etl_user",
  "password": "replace-me"
}
```

The Redshift database identity must have only the required target privileges,
for example for append-only loading:

```sql
GRANT USAGE ON SCHEMA risk TO etl_user;
GRANT INSERT ON TABLE risk.customer_result TO etl_user;
```

Adapt the SQL to your Redshift user/role model.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
npm install -g aws-cdk
cdk bootstrap
```

## Synthesize

Replace all example IDs and ARNs:

```bash
cdk synth \
  -c environment=dev \
  -c project_name=customer-risk \
  -c vpc_id=vpc-xxxxxxxx \
  -c private_subnet_id=subnet-xxxxxxxx \
  -c redshift_security_group_id=sg-xxxxxxxx \
  -c redshift_host=my-cluster.xxxxxx.ca-central-1.redshift.amazonaws.com \
  -c redshift_port=5439 \
  -c redshift_database=analytics \
  -c redshift_target_schema=risk \
  -c redshift_target_table=customer_result \
  -c redshift_secret_arn=arn:aws:secretsmanager:ca-central-1:123456789012:secret:prod/redshift/customer-etl-AbCdEf \
  -c source_database=customer_curated \
  -c source_table=customer
```

## Deploy

```bash
cdk deploy \
  -c environment=dev \
  -c project_name=customer-risk \
  -c vpc_id=vpc-xxxxxxxx \
  -c private_subnet_id=subnet-xxxxxxxx \
  -c redshift_security_group_id=sg-xxxxxxxx \
  -c redshift_host=my-cluster.xxxxxx.ca-central-1.redshift.amazonaws.com \
  -c redshift_database=analytics \
  -c redshift_target_schema=risk \
  -c redshift_target_table=customer_result \
  -c redshift_secret_arn=arn:aws:secretsmanager:ca-central-1:123456789012:secret:prod/redshift/customer-etl-AbCdEf \
  -c source_database=customer_curated \
  -c source_table=customer
```

## Important architecture notes

- The source is read through the Glue Catalog so Lake Formation can enforce the
  table grant.
- The Glue job role is not given direct read access to the source S3 bucket in
  this stack.
- The S3 temporary bucket is technical staging and is controlled by IAM rather
  than Lake Formation.
- This stack grants only `DESCRIBE` on the source database and
  `SELECT`/`DESCRIBE` on the one source table.
- It does not grant `CREATE_TABLE`, `ALTER`, `DROP`, `INSERT`, or
  `DATA_LOCATION_ACCESS`.
- IAM API permissions and Lake Formation data permissions are both required.
- Redshift database privileges are separate from AWS IAM permissions.
- The source location must already be registered with Lake Formation.
- Depending on the organization's selected Lake Formation access mode, the
  platform team may also need to enable the relevant application-integration
  setting for Glue full-table access.
