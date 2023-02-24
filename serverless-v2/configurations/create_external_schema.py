import redshift_connector
import boto3
import yaml
import json

rs_client = boto3.client('redshift')
with open('replay.yaml','r') as fr:
    config_read = yaml.safe_load(fr)
target_cluster_endpoint = config_read['target_cluster_endpoint']
cluster_endpoint_split = target_cluster_endpoint.split(".")
workgroup_id = cluster_endpoint_split[0]
db_host = target_cluster_endpoint.split(":")[0]
db_port = cluster_endpoint_split[5].split("/")[0][4:]
db_name = cluster_endpoint_split[5].split("/")[1]
db_username = config_read['master_username']
serverless_cluster_id = f"redshift-serverless-{workgroup_id}"
with open('system_config.json','r') as jr:
    json_data = json.load(jr)
script = json_data['EXTERNAL_SCHEMA_SCRIPT']
try:
    response = rs_client.get_cluster_credentials(
                        DbUser=db_username, ClusterIdentifier=serverless_cluster_id, AutoCreate=False,
                        DurationSeconds=3600
                    )
except rs_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ExpiredToken':
            print(f"Error retrieving credentials for {serverless_cluster_id}: IAM credentials have expired.")
            exit(-1)
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"Serverless endpoint could not be found "
                        f"RedshiftServerless:GetCredentials. {e}")
            exit(-1)
        else:
            print(f"Got exception retrieving credentials ({e.response['Error']['Code']})")
            raise e
db_user = response['DbUser']
db_password = response['DbPassword']
try:
    conn = redshift_connector.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password
        )
    cursor = conn.cursor()
    conn.autocommit = True
    cursor.execute(script)
    print(f"Executed script.{script}")
except Exception as err:
    if "already exists" not in str(err):
        print(f"Got exception while executing script {err}")
        raise
