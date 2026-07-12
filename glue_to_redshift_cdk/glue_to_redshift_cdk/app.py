#!/usr/bin/env python3
import os

import aws_cdk as cdk

from glue_to_redshift_cdk.glue_to_redshift_stack import (
    GlueToRedshiftConfig,
    GlueToRedshiftStack,
)


app = cdk.App()

# In a real bank project these values normally come from:
#   * CDK context (-c key=value)
#   * environment-specific configuration files
#   * a pipeline variable group / parameter store
#
# They are kept explicit here so the security and deployment boundaries
# remain easy to review.
config = GlueToRedshiftConfig(
    environment_name=app.node.try_get_context("environment") or "dev",
    project_name=app.node.try_get_context("project_name") or "customer-risk",

    # Existing network resources managed by the platform/network stack.
    vpc_id=app.node.try_get_context("vpc_id") or "vpc-0123456789abcdef0",
    private_subnet_id=(
        app.node.try_get_context("private_subnet_id")
        or "subnet-0123456789abcdef0"
    ),
    redshift_security_group_id=(
        app.node.try_get_context("redshift_security_group_id")
        or "sg-0123456789abcdef0"
    ),

    # Existing Redshift endpoint and database.
    redshift_host=(
        app.node.try_get_context("redshift_host")
        or "example-cluster.abc123.ca-central-1.redshift.amazonaws.com"
    ),
    redshift_port=int(app.node.try_get_context("redshift_port") or 5439),
    redshift_database=(
        app.node.try_get_context("redshift_database") or "analytics"
    ),
    redshift_target_schema=(
        app.node.try_get_context("redshift_target_schema") or "risk"
    ),
    redshift_target_table=(
        app.node.try_get_context("redshift_target_table") or "customer_result"
    ),

    # Existing Secrets Manager secret containing at least:
    #   {"username": "...", "password": "..."}
    redshift_secret_arn=(
        app.node.try_get_context("redshift_secret_arn")
        or "arn:aws:secretsmanager:ca-central-1:123456789012:"
           "secret:prod/redshift/customer-etl-AbCdEf"
    ),

    # Existing Glue Catalog/Lake Formation source table.
    source_database=(
        app.node.try_get_context("source_database") or "customer_curated"
    ),
    source_table=app.node.try_get_context("source_table") or "customer",

    # Glue capacity and operational settings.
    glue_version=app.node.try_get_context("glue_version") or "5.0",
    worker_type=app.node.try_get_context("worker_type") or "G.1X",
    number_of_workers=int(app.node.try_get_context("number_of_workers") or 5),
    timeout_minutes=int(app.node.try_get_context("timeout_minutes") or 60),

    # Optional schedule. Leave as None for an externally orchestrated job.
    # Example: "cron(0 6 * * ? *)"
    schedule_expression=app.node.try_get_context("schedule_expression"),

    # Notifications can be connected later through the exported SNS topic ARN.
    alarm_email=app.node.try_get_context("alarm_email"),
)

GlueToRedshiftStack(
    app,
    f"{config.project_name}-{config.environment_name}-glue-to-redshift",
    config=config,
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION", "ca-central-1"),
    ),
)

app.synth()
