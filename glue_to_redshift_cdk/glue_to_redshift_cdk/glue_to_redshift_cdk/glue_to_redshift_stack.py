from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as event_targets,
    aws_glue as glue,
    aws_iam as iam,
    aws_lakeformation as lakeformation,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_secretsmanager as secretsmanager,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
)
from constructs import Construct


@dataclass(frozen=True)
class GlueToRedshiftConfig:
    """
    Environment-specific inputs for the pipeline stack.

    The stack intentionally imports shared platform resources instead of
    creating a VPC, Redshift cluster, Glue database, or source table.

    This mirrors a common banking separation of duties:
      * Network/platform teams own VPC and Redshift.
      * Data governance owns Lake Formation administration.
      * The pipeline team owns the Glue job and its least-privilege grants.
    """

    environment_name: str
    project_name: str

    # Existing network resources.
    vpc_id: str
    private_subnet_id: str
    redshift_security_group_id: str

    # Existing Redshift target.
    redshift_host: str
    redshift_port: int
    redshift_database: str
    redshift_target_schema: str
    redshift_target_table: str
    redshift_secret_arn: str

    # Existing Lake Formation-managed source table.
    source_database: str
    source_table: str

    # Glue runtime settings.
    glue_version: str = "5.0"
    worker_type: str = "G.1X"
    number_of_workers: int = 5
    timeout_minutes: int = 60

    # Optional EventBridge schedule and email notification.
    schedule_expression: Optional[str] = None
    alarm_email: Optional[str] = None


class GlueToRedshiftStack(Stack):
    """
    Creates one deployable Glue-to-Redshift pipeline.

    DATA FLOW
    ---------
      Lake Formation-managed Glue Catalog table
                       |
                       v
                  AWS Glue 5.0
                PySpark transform
                       |
                       v
              S3 temporary staging
                       |
                       v
                   Redshift

    CREATED BY THIS STACK
    ---------------------
      * S3 script bucket
      * S3 Redshift temporary/staging bucket
      * Glue job IAM role
      * Glue-to-Redshift VPC security group
      * Glue JDBC connection
      * Lake Formation database/table grants
      * Glue PySpark job
      * CloudWatch log group, alarm, and SNS topic
      * Optional EventBridge schedule

    REFERENCED, NOT CREATED
    -----------------------
      * VPC and private subnet
      * Redshift endpoint and Redshift security group
      * Redshift credentials secret
      * Glue Catalog database/table
      * Lake Formation data-location registration for the source data
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config: GlueToRedshiftConfig,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.config = config
        prefix = self._name_prefix(config)

        # ------------------------------------------------------------------
        # 1. Import shared network resources.
        # ------------------------------------------------------------------
        # from_lookup() requires the target VPC to exist at synth time and
        # normally performs a context lookup. This is appropriate when the
        # platform VPC is managed by another stack/account team.
        vpc = ec2.Vpc.from_lookup(
            self,
            "ImportedVpc",
            vpc_id=config.vpc_id,
        )

        # Glue requires exactly one subnet in a JDBC connection.
        # A private subnet is normally preferred for bank workloads.
        private_subnet = ec2.Subnet.from_subnet_id(
            self,
            "ImportedPrivateSubnet",
            config.private_subnet_id,
        )

        # Import the Redshift security group as mutable because this pipeline
        # must add one inbound rule from the Glue job security group.
        redshift_sg = ec2.SecurityGroup.from_security_group_id(
            self,
            "ImportedRedshiftSecurityGroup",
            config.redshift_security_group_id,
            mutable=True,
        )

        # ------------------------------------------------------------------
        # 2. Create dedicated S3 buckets for code and temporary Redshift data.
        # ------------------------------------------------------------------
        # Script bucket stores the Glue PySpark file deployed with CDK.
        # It is not a business-data lake bucket and is not registered with LF.
        script_bucket = s3.Bucket(
            self,
            "GlueScriptBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # Glue's Redshift connector uses an S3 temporary directory while
        # performing bulk load operations. This is a technical staging area,
        # not a business-facing governed dataset, so ordinary IAM/S3 control
        # is usually sufficient.
        staging_bucket = s3.Bucket(
            self,
            "RedshiftStagingBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=False,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="DeleteTemporaryRedshiftFiles",
                    enabled=True,
                    expiration=Duration.days(7),
                    abort_incomplete_multipart_upload_after=Duration.days(1),
                )
            ],
            removal_policy=RemovalPolicy.RETAIN,
        )

        # Deploy the local script directory into the script bucket.
        # BucketDeployment creates an asset and uploads it during deployment.
        script_deployment = s3deploy.BucketDeployment(
            self,
            "DeployGlueScripts",
            sources=[s3deploy.Source.asset("glue_scripts")],
            destination_bucket=script_bucket,
            destination_key_prefix="jobs",
            prune=True,
        )

        script_s3_uri = (
            f"s3://{script_bucket.bucket_name}/jobs/"
            "customer_to_redshift.py"
        )
        temp_s3_uri = (
            f"s3://{staging_bucket.bucket_name}/"
            f"{config.project_name}/{config.environment_name}/temp/"
        )

        # ------------------------------------------------------------------
        # 3. Import the Redshift credential secret.
        # ------------------------------------------------------------------
        # The secret should contain:
        #   {"username": "etl_user", "password": "strong-password"}
        #
        # The password is not embedded in the CDK template or Glue script.
        redshift_secret = secretsmanager.Secret.from_secret_complete_arn(
            self,
            "ImportedRedshiftSecret",
            config.redshift_secret_arn,
        )

        # ------------------------------------------------------------------
        # 4. Create the Glue job runtime IAM role.
        # ------------------------------------------------------------------
        glue_job_role = iam.Role(
            self,
            "GlueJobRole",
            role_name=f"{prefix}-glue-job-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description=(
                "Least-privilege runtime role for the Glue-to-Redshift job"
            ),
        )

        # CloudWatch Logs permissions.
        glue_job_role.add_to_policy(
            iam.PolicyStatement(
                sid="WriteGlueLogs",
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:AssociateKmsKey",
                ],
                resources=["*"],
            )
        )

        # Glue Catalog IAM permissions.
        #
        # Lake Formation permissions decide whether the role may access the
        # database/table data asset. IAM permissions still decide whether the
        # role may call the Glue and Lake Formation APIs.
        account = Stack.of(self).account
        region = Stack.of(self).region
        catalog_arn = f"arn:aws:glue:{region}:{account}:catalog"
        database_arn = (
            f"arn:aws:glue:{region}:{account}:database/"
            f"{config.source_database}"
        )
        table_arn = (
            f"arn:aws:glue:{region}:{account}:table/"
            f"{config.source_database}/{config.source_table}"
        )

        glue_job_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadSourceCatalogMetadata",
                actions=[
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition",
                ],
                resources=[catalog_arn, database_arn, table_arn],
            )
        )

        # Required when an LF-integrated engine asks Lake Formation for
        # temporary credentials scoped to the authorized table/location.
        glue_job_role.add_to_policy(
            iam.PolicyStatement(
                sid="RequestLakeFormationDataCredentials",
                actions=["lakeformation:GetDataAccess"],
                resources=["*"],
            )
        )

        # Glue connection lookup.
        glue_job_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadGlueConnection",
                actions=["glue:GetConnection", "glue:GetConnections"],
                resources=["*"],
            )
        )

        # VPC execution permissions used by Glue to create and remove ENIs.
        #
        # These APIs do not support useful resource-level restrictions for
        # all actions, so AWS commonly requires Resource "*". Conditions can
        # be added in a bank's central IAM boundary if required.
        glue_job_role.add_to_policy(
            iam.PolicyStatement(
                sid="ManageGlueVpcNetworkInterfaces",
                actions=[
                    "ec2:DescribeVpcEndpoints",
                    "ec2:DescribeRouteTables",
                    "ec2:CreateNetworkInterface",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DescribeSecurityGroups",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeVpcAttribute",
                    "ec2:DescribeVpcs",
                    "ec2:AssignPrivateIpAddresses",
                    "ec2:UnassignPrivateIpAddresses",
                ],
                resources=["*"],
            )
        )

        # Read the Redshift username/password from Secrets Manager.
        redshift_secret.grant_read(glue_job_role)

        # Read the deployed Glue script.
        script_bucket.grant_read(glue_job_role)

        # Read/write/delete temporary load files under the staging bucket.
        # The bucket lifecycle rule also removes abandoned files after 7 days.
        staging_bucket.grant_read_write(glue_job_role)
        glue_job_role.add_to_policy(
            iam.PolicyStatement(
                sid="AbortStagingMultipartUploads",
                actions=["s3:AbortMultipartUpload"],
                resources=[f"{staging_bucket.bucket_arn}/*"],
            )
        )

        # ------------------------------------------------------------------
        # 5. Create security groups and Glue JDBC connection.
        # ------------------------------------------------------------------
        glue_sg = ec2.SecurityGroup(
            self,
            "GlueJobSecurityGroup",
            vpc=vpc,
            security_group_name=f"{prefix}-glue-sg",
            description=(
                "Security group used by Glue Spark ENIs to reach Redshift"
            ),
            allow_all_outbound=True,
        )

        # Glue Spark drivers/executors require communication among ENIs.
        # The broad self-referencing TCP rule is the standard Glue pattern.
        glue_sg.add_ingress_rule(
            peer=glue_sg,
            connection=ec2.Port.all_tcp(),
            description="Allow Glue Spark nodes to communicate with each other",
        )

        # Allow only the Glue job SG to reach the Redshift listener port.
        redshift_sg.add_ingress_rule(
            peer=glue_sg,
            connection=ec2.Port.tcp(config.redshift_port),
            description=(
                f"Allow {prefix} Glue job to connect to Redshift"
            ),
        )

        jdbc_url = (
            f"jdbc:redshift://{config.redshift_host}:"
            f"{config.redshift_port}/{config.redshift_database}"
        )

        # SECRET_ID keeps database credentials out of CloudFormation.
        # Glue reads the secret at runtime using the job role.
        redshift_connection = glue.CfnConnection(
            self,
            "GlueRedshiftConnection",
            catalog_id=account,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name=f"{prefix}-redshift-connection",
                description=(
                    "Private JDBC connection used by the Glue job "
                    "to load Amazon Redshift"
                ),
                connection_type="JDBC",
                connection_properties={
                    "JDBC_CONNECTION_URL": jdbc_url,
                    "SECRET_ID": config.redshift_secret_arn,
                },
                physical_connection_requirements=(
                    glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                        subnet_id=private_subnet.subnet_id,
                        security_group_id_list=[
                            glue_sg.security_group_id
                        ],
                        # Availability zone is intentionally omitted. Glue can
                        # infer it from the selected subnet.
                    )
                ),
            ),
        )

        # ------------------------------------------------------------------
        # 6. Grant Lake Formation permissions on the source catalog objects.
        # ------------------------------------------------------------------
        # Database DESCRIBE lets the role discover/resolve the database.
        lf_database_grant = lakeformation.CfnPermissions(
            self,
            "LakeFormationSourceDatabaseGrant",
            data_lake_principal=(
                lakeformation.CfnPermissions.DataLakePrincipalProperty(
                    data_lake_principal_identifier=glue_job_role.role_arn
                )
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=(
                    lakeformation.CfnPermissions.DatabaseResourceProperty(
                        catalog_id=account,
                        name=config.source_database,
                    )
                )
            ),
            permissions=["DESCRIBE"],
        )

        # Table SELECT authorizes reading the full table.
        #
        # We deliberately do not grant CREATE_TABLE, ALTER, DROP, INSERT, or
        # DATA_LOCATION_ACCESS because this job only reads an existing LF
        # table and writes to Redshift.
        lf_table_grant = lakeformation.CfnPermissions(
            self,
            "LakeFormationSourceTableGrant",
            data_lake_principal=(
                lakeformation.CfnPermissions.DataLakePrincipalProperty(
                    data_lake_principal_identifier=glue_job_role.role_arn
                )
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                table_resource=(
                    lakeformation.CfnPermissions.TableResourceProperty(
                        catalog_id=account,
                        database_name=config.source_database,
                        name=config.source_table,
                    )
                )
            ),
            permissions=["SELECT", "DESCRIBE"],
        )

        # Ensure database permission is established before table permission.
        lf_table_grant.add_dependency(lf_database_grant)

        # IMPORTANT:
        # This stack assumes the source table's S3 location is already
        # registered in Lake Formation by the data-platform stack.
        #
        # A consumer pipeline should normally not re-register a shared source
        # bucket because registration is an administrative platform concern.

        # ------------------------------------------------------------------
        # 7. Create log group and Glue PySpark job.
        # ------------------------------------------------------------------
        log_group = logs.LogGroup(
            self,
            "GlueJobLogGroup",
            log_group_name=f"/aws-glue/jobs/{prefix}",
            retention=logs.RetentionDays.THREE_MONTHS,
            removal_policy=RemovalPolicy.RETAIN,
        )

        glue_job = glue.CfnJob(
            self,
            "GlueJob",
            name=f"{prefix}-glue-job",
            description=(
                "Reads a Lake Formation-managed Glue table, transforms it, "
                "and loads the result into Amazon Redshift"
            ),
            role=glue_job_role.role_arn,
            glue_version=config.glue_version,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=script_s3_uri,
            ),
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[redshift_connection.ref]
            ),
            worker_type=config.worker_type,
            number_of_workers=config.number_of_workers,
            timeout=config.timeout_minutes,
            max_retries=0,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-metrics": "true",
                "--enable-observability-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-job-insights": "true",
                "--TempDir": temp_s3_uri,
                "--SOURCE_DATABASE": config.source_database,
                "--SOURCE_TABLE": config.source_table,
                "--REDSHIFT_CONNECTION_NAME": redshift_connection.ref,
                "--REDSHIFT_DATABASE": config.redshift_database,
                "--TARGET_SCHEMA": config.redshift_target_schema,
                "--TARGET_TABLE": config.redshift_target_table,
                "--JOB_NAME": f"{prefix}-glue-job",
            },
            tags={
                "Project": config.project_name,
                "Environment": config.environment_name,
                "DataClassification": "Internal",
                "ManagedBy": "AWS-CDK",
            },
        )

        # The job script must exist before the job can run. CloudFormation can
        # technically create the Job before deployment completes, so an
        # explicit dependency makes the intent clear.
        glue_job.node.add_dependency(script_deployment)
        glue_job.add_dependency(redshift_connection)
        glue_job.add_dependency(lf_table_grant)

        # ------------------------------------------------------------------
        # 8. Operational alerting.
        # ------------------------------------------------------------------
        alarm_topic = sns.Topic(
            self,
            "GlueJobAlarmTopic",
            topic_name=f"{prefix}-alerts",
            display_name=f"{prefix} Glue job alerts",
        )

        if config.alarm_email:
            # The recipient must confirm the SNS subscription by email.
            alarm_topic.add_subscription(
                sns_subscriptions.EmailSubscription(config.alarm_email)
            )

        # AWS/Glue publishes one metric dimension per job name.
        failed_runs_metric = cloudwatch.Metric(
            namespace="Glue",
            metric_name="glue.driver.aggregate.numFailedTasks",
            dimensions_map={"JobName": glue_job.ref},
            statistic="Sum",
            period=Duration.minutes(5),
        )

        failure_alarm = cloudwatch.Alarm(
            self,
            "GlueJobFailureAlarm",
            alarm_name=f"{prefix}-glue-failed-tasks",
            alarm_description=(
                "Glue reported one or more failed Spark tasks. "
                "Confirm job-run status and inspect Glue logs."
            ),
            metric=failed_runs_metric,
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        failure_alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(alarm_topic)
        )

        # ------------------------------------------------------------------
        # 9. Optional EventBridge schedule.
        # ------------------------------------------------------------------
        if config.schedule_expression:
            schedule_rule = events.Rule(
                self,
                "GlueJobSchedule",
                rule_name=f"{prefix}-schedule",
                description=f"Schedule for {glue_job.ref}",
                schedule=events.Schedule.expression(
                    config.schedule_expression
                ),
            )
            schedule_rule.add_target(
                event_targets.AwsApi(
                    service="Glue",
                    action="startJobRun",
                    parameters={"JobName": glue_job.ref},
                    policy_statement=iam.PolicyStatement(
                        actions=["glue:StartJobRun"],
                        resources=[
                            f"arn:aws:glue:{region}:{account}:job/{glue_job.ref}"
                        ],
                    ),
                )
            )

        # ------------------------------------------------------------------
        # 10. Outputs useful to CI/CD and operations teams.
        # ------------------------------------------------------------------
        CfnOutput(
            self,
            "GlueJobName",
            value=glue_job.ref,
            description="Name of the deployed Glue job",
        )
        CfnOutput(
            self,
            "GlueJobRoleArn",
            value=glue_job_role.role_arn,
            description="Runtime IAM role used by the Glue job",
        )
        CfnOutput(
            self,
            "GlueConnectionName",
            value=redshift_connection.ref,
            description="Glue JDBC connection name",
        )
        CfnOutput(
            self,
            "StagingBucketName",
            value=staging_bucket.bucket_name,
            description="Temporary S3 bucket used by the Redshift load",
        )
        CfnOutput(
            self,
            "AlarmTopicArn",
            value=alarm_topic.topic_arn,
            description="SNS topic for operational alarms",
        )

    @staticmethod
    def _name_prefix(config: GlueToRedshiftConfig) -> str:
        """Create a consistent, lower-case physical resource-name prefix."""
        raw = f"{config.project_name}-{config.environment_name}"
        return raw.lower().replace("_", "-")
