#!/bin/sh
# This script bootstraps a base amazonlinux image to run the Redshift
# Node Config concurrency test.
# 1. Install the AWS CLI, Python3, and necessary Python libraries.
# 2. Copy Python program source for concurrency test
# 3. Execute that Python program
# We expect all configuration to be defined as environment variables
# for the Batch job.

set -eu

yum install -y awscli python3
pip3 install boto3 psycopg2-binary pandas sqlalchemy

aws s3 cp "$PYTHON_SCRIPT" ./script.py

# This Python program requires these environment variables to be set:
# `$SQL_SCRIPT_S3_PATH`, `$REDSHIFT_CLUSTER_ENDPOINT`,
# `$REDSHIFT_IAM_ROLE`, `$BUCKET_NAME`, `$REDSHIFT_USER_NAME`
python3 ./script.py
