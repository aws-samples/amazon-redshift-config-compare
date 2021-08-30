#!/bin/bash
set -e
echo "bucket_name: $BUCKET_NAME"
echo "simple_replay_overwrite_s3_path: $SIMPLE_REPLAY_OVERWRITE_S3_PATH"
echo "redshift_user_name: $REDSHIFT_USER_NAME"
echo "what_if_timestamp: $WHAT_IF_TIMESTAMP"
echo "extract_prefix: $EXTRACT_PREFIX"
echo "replay_prefix: $REPLAY_PREFIX"
echo "redshift_iam_role: $REDSHIFT_IAM_ROLE"
echo "workload_location: $WORKLOAD_LOCATION"
echo "cluster_endpoint: $CLUSTER_ENDPOINT"
echo "cluster_identifier: $CLUSTER_IDENTIFIER"

yum update -y
yum -y install git
yum -y install python3
yum -y install python3-pip
yum -y install aws-cfn-bootstrap
yum -y install gcc gcc-c++ python3 python3-devel unixODBC unixODBC-devel
# mkdir /root/.aws
# echo "[default]" > /root/.aws/config
# echo "region = ${AWS::Region}" >> /root/.aws/config
mkdir /amazonutils
cd /amazonutils
git clone https://github.com/awslabs/amazon-redshift-utils.git
pip3 install -r /amazonutils/amazon-redshift-utils/src/SimpleReplay/requirements.txt
#
# configure extract replay metadata
#
cd /amazonutils/amazon-redshift-utils/src/SimpleReplay
# if ! [ -z "$SIMPLE_REPLAY_OVERWRITE_S3_PATH" ]; then
#   aws s3 cp $SIMPLE_REPLAY_OVERWRITE_S3_PATH replay.yaml
# fi


sed -i "s#master_username: \"\"#master_username: \"$REDSHIFT_USER_NAME\"#g" replay.yaml
sed -i "s#execute_unload_statements: \"false\"#execute_unload_statements: \"true\"#g" replay.yaml
sed -i "s#unload_iam_role: \"\"#unload_iam_role: \"$REDSHIFT_IAM_ROLE\"#g" replay.yaml
sed -i "s#target_cluster_system_table_unload_iam_role: \"\"#target_cluster_system_table_unload_iam_role: \"$REDSHIFT_IAM_ROLE\"#g" replay.yaml
sed -i "s#workload_location: \"\"#workload_location: \"$WORKLOAD_LOCATION\"#g" replay.yaml
sed -i "s#target_cluster_endpoint: \"\"#target_cluster_endpoint: \"$CLUSTER_ENDPOINT\"#g"  replay.yaml
sed -i "s#replay_output: \"\"#replay_output: \"s3://$BUCKET_NAME/$REPLAY_PREFIX/$WHAT_IF_TIMESTAMP/$CLUSTER_IDENTIFIER\"#g" replay.yaml

account_id=`aws sts get-caller-identity --query Account --output text`

if [[ "$account_id" == "$SNAPSHOT_ACCOUNT_ID" ]]; then
   sed -i "s#execute_copy_statements: \"false\"#execute_copy_statements: \"true\"#g" replay.yaml
   aws s3 cp $WORKLOAD_LOCATION/copy_replacements.csv . || true
   sed -z -i "s#,,\n#,,$redshift_iam_role\n#g" copy_replacements.csv || true
   aws s3 cp copy_replacements.csv $WORKLOAD_LOCATION/copy_replacements.csv || true
fi
python3 replay.py ./$bucket_keyprefix/replay.yaml
