from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_sources,
    aws_glue as glue,
    aws_iam as iam,
    aws_lakeformation as lakeformation,
    CfnOutput
)
from constructs import Construct


class GluePipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, environment: str = "dev", **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # =====================================================================
        # 1. 存储与消息缓冲层 (Storage & Messaging)
        # =====================================================================

        # 原始数据桶 (DataBucket)
        data_bucket = s3.Bucket(
            self, "DataBucket",
            bucket_name=f"etl-data-bucket-{environment}-{self.account}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN  # 金融级数据保护，防止误删
        )

        # 产物与分布式锁桶 (ProcessedBucket)
        processed_bucket = s3.Bucket(
            self, "ProcessedBucket",
            bucket_name=f"etl-processed-bucket-{environment}-{self.account}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    prefix="_locks/",
                    expiration=Duration.days(7),
                    description="7天自动过期释放分布式死锁"
                )
            ]
        )

        # 死信队列 (DLQ)
        dlq = sqs.Queue(
            self, "IngestionDLQ",
            queue_name=f"etl-ingestion-dlq-{environment}",
            retention_period=Duration.days(14)
        )

        # 主摄取队列 (IngestionQueue)
        ingestion_queue = sqs.Queue(
            self, "IngestionQueue",
            queue_name=f"etl-ingestion-queue-{environment}",
            visibility_timeout=Duration.seconds(900),  # 匹配 Lambda 超时
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=5,
                queue=dlq
            )
        )

        # 核心纽带：绑定 S3 事件通知到 SQS (仅当 _COMPLETE 哨兵文件上传时触发)
        # CDK 会在底层自动为你创建和配置复杂的 SQS Queue Policy
        data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3.BucketNotificationDestination.SQS(ingestion_queue),
            s3.NotificationKeyFilter(suffix="_COMPLETE")
        )

        # =====================================================================
        # 2. 计算与元数据层 (Compute & Catalog)
        # =====================================================================

        # Glue 元数据中央库
        glue_db = glue.CfnDatabase(
            self, "ProcessedDataDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"etl_processed_data_{environment}",
                description="Database for processed Parquet data governed by Lake Formation"
            )
        )

        # Glue 作业与爬虫的角色 (GlueJobRole)
        glue_job_role = iam.Role(
            self, "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        # 显式授予 Glue 读写 S3 的最小特权
        processed_bucket.grant_read_write(glue_job_role)
        data_bucket.grant_read(glue_job_role)

        # AWS Glue 5.0 (Serverless Apache Spark) 作业
        glue_job = glue.CfnJob(
            self, "CsvEtlGlueJob",
            name=f"csv-etl-job-{environment}",
            role=glue_job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{data_bucket.bucket_name}/scripts/csv_etl_job.py"
            ),
            glue_version="5.0",  # 拥抱最新 Glue 5.0
            worker_type="G.1X",
            number_of_workers=2,
            timeout=60,
            default_arguments={
                "--job-bookmark-option": "job-bookmark-disable",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--PROCESSED_BUCKET": processed_bucket.bucket_name
            }
        )

        # Glue 动态 Schema 发现爬虫 (Crawler)
        glue_crawler = glue.CfnCrawler(
            self, "ProcessedDataCrawler",
            name=f"processed-data-crawler-{environment}",
            role=glue_job_role.role_arn,
            database_name=glue_db.database_input.name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(path=f"s3://{processed_bucket.bucket_name}/output/")]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="DEPRECATE_IN_DATABASE"
            ),
            configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}}}'
        )

        # 允许 Glue 作业拥有拉起爬虫的控制权限
        glue_job_role.add_to_policy(iam.PolicyStatement(
            actions=["glue:StartCrawler"],
            resources=[f"arn:aws:glue:{self.region}:{self.account}:crawler/{glue_crawler.name}"]
        ))

        # =====================================================================
        # 3. 事件驱动流控层 (Event Orchestration)
        # =====================================================================

        # Lambda 调度员角色
        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        # 授予 Lambda 读写分布式锁以及触发 Glue 的最小特权
        processed_bucket.grant_read_write(lambda_role)
        data_bucket.grant_read(lambda_role)
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=["glue:StartJobRun"],
            resources=[f"arn:aws:glue:{self.region}:{self.account}:job/{glue_job.name}"]
        ))

        # 核心 Lambda 函数 (指挥官)
        glue_trigger_lambda = _lambda.Function(
            self, "GlueTriggerLambda",
            function_name=f"etl-glue-trigger-{environment}",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="glue_trigger.lambda_handler",
            code=_lambda.Code.from_asset("lambda/"),  # 自动打包本地的 lambda 目录并上传至 S3
            role=lambda_role,
            timeout=Duration.minutes(15),
            environment={
                "GLUE_JOB_NAME": glue_job.name,
                "LOCK_BUCKET": processed_bucket.bucket_name
            }
        )

        # 将 SQS 消息队列无缝挂载为 Lambda 的触发源
        glue_trigger_lambda.add_event_source(lambda_sources.SqsEventSource(
            ingestion_queue,
            batch_size=1
        ))

        # =====================================================================
        # 4. 金融级数据治理层 (AWS Lake Formation)
        # =====================================================================

        # 将 S3 处理桶注册进 Lake Formation 数据湖
        lf_data_location = lakeformation.CfnResource(
            self, "LakeFormationProcessedDataLocation",
            resource_arn=processed_bucket.bucket_arn,
            use_service_linked_role=True
        )

        # 赋予 Glue Job 在 Catalog 数据库中的数据湖操作特权 (代替原生的 IAM 放任策略)
        glue_job_lf_grant = lakeformation.CfnPermissions(
            self, "GlueJobLakeFormationDatabaseGrant",
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=glue_job_role.role_arn
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(
                    name=glue_db.database_input.name
                )
            ),
            permissions=["CREATE_TABLE", "ALTER", "DROP", "DESCRIBE"]
        )

        # 联动输出参数 (Outputs)
        CfnOutput(self, "DataBucketName", value=data_bucket.bucket_name)
        CfnOutput(self, "ProcessedBucketName", value=processed_bucket.bucket_name)
        CfnOutput(self, "GlueJobName", value=glue_job.name)