import boto3
import time
import traceback
import botocore.exceptions as be
import json
import re
import os

def handler(event, context):
    print(event)
    action = event['Input'].get('action')

    user_config = get_json_config_from_s3(os.environ['USER_CONFIG_JSON_S3_PATH'])
    system_config = get_json_config_from_s3(os.environ['SYSTEM_CONFIG_JSON_S3_PATH'])
    cluster_identifier_prefix = os.environ['CLUSTER_IDENTIFIER_PREFIX']

    what_if_timestamp = event['Input'].get('what_if_timestamp')
    cluster_identifier = event['Input'].get('cluster_identifier')
    sql_id = event['Input'].get('sql_id')
    job_id = event['Input'].get('job_id')
    redshift_cluster_configuration = event['Input'].get('redshift_cluster_configuration')
    redshift_cluster_index = event['Input'].get('redshift_cluster_index')

    try:

        client = boto3.client('redshift')
        if user_config.get('DATABASE_NAME') == 'N/A' or user_config.get('DATABASE_NAME') is None :
            database_name = system_config.get('DATABASE_NAME')
            print("Database name from system_config")
        else:
            database_name = user_config.get('DATABASE_NAME')
            print("Database name from user_config")
        print("Database name {}". format(database_name))    
        if action == "initiate":
            what_if_timestamp = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(time.time()))
            res = {'status': what_if_timestamp}
        elif action == "run_extract":
            res = {
                'job_id': run_extract(
                    what_if_timestamp=what_if_timestamp,
                    simple_replay_log_location=user_config.get('SIMPLE_REPLAY_LOG_LOCATION'),
                    simple_replay_extract_start_time=user_config.get('SIMPLE_REPLAY_EXTRACT_START_TIME'),
                    simple_replay_extract_end_time=user_config.get('SIMPLE_REPLAY_EXTRACT_END_TIME'),
                    simple_replay_extract_overwrite_s3_path=user_config.get('SIMPLE_REPLAY_EXTRACT_OVERWRITE_S3_PATH'),
                    bucket_name=system_config.get('S3_BUCKET_NAME'),
                    redshift_user_name=system_config.get('MASTER_USER_NAME'),
                    extract_prefix=system_config.get('EXTRACT_PREFIX'),
                    script_prefix=system_config.get('SCRIPT_PREFIX'),
                    extract_bootstrap_script=system_config.get('EXTRACT_BOOTSTRAP_SCRIPT'),
                    job_definition=system_config.get('JOB_DEFINITION'),
                    job_queue=system_config.get('JOB_QUEUE')
                )}
        elif action == "batch_job_status":
            res = {'status': batch_job_status(job_id=job_id)}
        elif action == "get_redshift_configurations":
            res = {'status': user_config.get('CONFIGURATIONS')}
        elif action == "get_cluster_identifier":
            res = {'status': get_cluster_identifier(client, user_config, redshift_cluster_configuration,
                                                    cluster_identifier_prefix)}
        elif action == "cluster_status":
            res = {'status': cluster_status(client, cluster_identifier)}
        elif action == "create_parameter_group":
            res = {'status': create_parameter_group(client, cluster_identifier)}
        elif action == "update_parameter_group":

            if user_config.get('PARAMETER_GROUP_CONFIG_S3_PATH') is None or user_config.get(
                    'PARAMETER_GROUP_CONFIG_S3_PATH') == "N/A":
                parameter_group = system_config.get('PARAMETER_GROUP_CONFIG')
            else:
                parameter_group = user_config.get('PARAMETER_GROUP_CONFIG_S3_PATH')

            res = {'status': update_parameter_group(client, cluster_identifier, parameter_group)}

        elif action == "create_cluster":
            res = {
                'status': create_cluster(client,
                                         cluster_identifier,
                                         user_config.get('SNAPSHOT_ID'),
                                         system_config.get('REDSHIFT_IAM_ROLE'),
                                         cluster_identifier,
                                         system_config.get('SUBNET_GROUP'),
                                         system_config.get('SECURITY_GROUP_ID'),
                                         user_config.get('SNAPSHOT_ACCOUNT_ID'),
                                         redshift_cluster_configuration.get('NODE_TYPE'),
                                         redshift_cluster_configuration.get('NUMBER_OF_NODES'),
                                         master_user_name=system_config.get('MASTER_USER_NAME'),
                                         database_name=database_name,
                                         secrets_manager_arn=system_config.get('SECRETS_MANAGER_ARN'),
                                         port=int(system_config.get('PORT')),
                                         publicly_accessible=(system_config.get('PUBLICLY_ACCESSIBLE')=="true")
                                         )}

        elif action == "classic_resize_cluster":
            res = {'status': classic_resize_cluster(client, cluster_identifier,
                                                    redshift_cluster_configuration.get('NODE_TYPE'),
                                                    redshift_cluster_configuration.get('NUMBER_OF_NODES'))}
        elif action == "resume_cluster":
            client.resume_cluster(ClusterIdentifier=cluster_identifier)
            res = {'status': 'initiated'}
        elif action == "pause_cluster":
            res = {
                'status': pause_cluster(client=client,
                                        cluster_identifier=cluster_identifier,
                                        redshift_cluster_index=redshift_cluster_index,
                                        auto_pause=user_config.get('AUTO_PAUSE'))}
        elif action == "update_wlm_config":
            res = {'status': update_wlm_config(client, cluster_identifier,
                                               redshift_cluster_configuration.get('WLM_CONFIG_S3_PATH'))}
                                               
        ## Added to check for clusters in pending reboot after wlm change ##
        elif action == "check_pending_reboot_status":
            res = {'status': check_pending_reboot_status(client, cluster_identifier) }
        
        elif action == "run_ddl_and_copy_script":
            res = {
                'sql_id': run_sql_script_from_s3(script_s3_path=user_config.get('DDL_AND_COPY_SCRIPT_S3_PATH'),
                                                 action=action,
                                                 cluster_identifier=cluster_identifier,
                                                 redshift_iam_role=system_config.get('REDSHIFT_IAM_ROLE'),
                                                 bucket_name=system_config.get('S3_BUCKET_NAME'),
                                                 db=database_name,
                                                 user=system_config.get('MASTER_USER_NAME'))}
        elif action == "run_redshift_performance_test":
            res = {
                'job_id': run_redshift_performance_test(
                    client=client,
                    cluster_identifier=cluster_identifier,
                    bucket_name=system_config.get('S3_BUCKET_NAME'),
                    performance_test_bootstrap_script=system_config.get('PERFORMANCE_TEST_BOOTSTRAP_SCRIPT'),
                    performance_test_python_script=system_config.get('PERFORMANCE_TEST_PYTHON_SCRIPT'),
                    sql_script_s3_path=user_config.get('SQL_SCRIPT_S3_PATH'),
                    number_of_parallel_sessions_list=user_config.get('NUMBER_OF_PARALLEL_SESSIONS_LIST'),
                    job_definition=system_config.get('JOB_DEFINITION'),
                    job_queue=system_config.get('JOB_QUEUE'),
                    redshift_iam_role=system_config.get('REDSHIFT_IAM_ROLE'),
                    redshift_user_name=system_config.get('MASTER_USER_NAME'),
                    db=database_name,
                    disable_result_cache=system_config.get('DISABLE_RESULT_CACHE'),
                    default_output_limit=system_config.get('DEFAULT_OUTPUT_LIMIT'),
                    max_number_of_queries=system_config.get('MAX_NUMBER_OF_QUERIES'),
                    max_parallel_sessions=system_config.get('MAX_PARALLEL_SESSIONS'),
                    query_label_prefix=system_config.get('QUERY_LABEL_PREFIX')

                )}
        elif action == "run_replay":
            res = {
                'job_id': run_replay(
                    client=client,
                    what_if_timestamp=what_if_timestamp,
                    cluster_identifier=cluster_identifier,
                    extract_s3_path='s3://' + system_config.get('S3_BUCKET_NAME') + '/' + system_config.get(
                        'EXTRACT_PREFIX') + '/' + what_if_timestamp + '/',
                    simple_replay_overwrite_s3_path=user_config.get('SIMPLE_REPLAY_OVERWRITE_S3_PATH'),
                    simple_replay_log_location=user_config.get('SIMPLE_REPLAY_LOG_LOCATION'),
                    bucket_name=system_config.get('S3_BUCKET_NAME'),
                    redshift_user_name=system_config.get('MASTER_USER_NAME'),
                    redshift_iam_role=system_config.get('REDSHIFT_IAM_ROLE'),
                    db=database_name,
                    extract_prefix=system_config.get('EXTRACT_PREFIX'),
                    replay_prefix=system_config.get('REPLAY_PREFIX'),
                    script_prefix=system_config.get('SCRIPT_PREFIX'),
                    snapshot_account_id=user_config.get('SNAPSHOT_ACCOUNT_ID'),
                    replay_bootstrap_script=system_config.get('REPLAY_BOOTSTRAP_SCRIPT'),
                    job_definition=system_config.get('JOB_DEFINITION'),
                    job_queue=system_config.get('JOB_QUEUE')
                )}
        elif action == "gather_comparison_stats":
            res = {'sql_id': gather_comparison_stats(script_s3_path=system_config.get('GATHER_COMPARISON_STATS_SCRIPT'),
                                                     action=action,
                                                     cluster_identifier=cluster_identifier,
                                                     redshift_iam_role=system_config.get('REDSHIFT_IAM_ROLE'),
                                                     bucket_name=system_config.get('S3_BUCKET_NAME'),
                                                     db=database_name,
                                                     user=system_config.get('MASTER_USER_NAME'),
                                                     run_type='sync',
                                                     what_if_timestamp=what_if_timestamp,
                                                     comparison_stats_s3_path=system_config.get(
                                                         'COMPARISON_STATS_S3_PATH'),
                                                     external_schema_script=system_config.get('EXTERNAL_SCHEMA_SCRIPT'),
                                                     query_label_prefix=system_config.get('QUERY_LABEL_PREFIX'),
                                                     node_type=redshift_cluster_configuration.get('NODE_TYPE'),
                                                     number_of_nodes=redshift_cluster_configuration.get('NUMBER_OF_NODES'),
                                                     region=system_config.get('REGION'),
                                                     cluster_config_s3_path=system_config.get('CLUSTER_CONFIG_S3_PATH'))
                   }
        elif action == "populate_comparison_results":
            res = {
                'sql_id': populate_comparison_results(
                    script_s3_path=system_config.get('POPULATE_COMPARISON_RESULTS_SCRIPT'),
                    action=action,
                    cluster_identifier=cluster_identifier,
                    redshift_iam_role=system_config.get('REDSHIFT_IAM_ROLE'),
                    bucket_name=system_config.get('S3_BUCKET_NAME'),
                    db=database_name,
                    user=system_config.get('MASTER_USER_NAME'),
                    what_if_timestamp=what_if_timestamp,
                    raw_comparison_results_s3_path=system_config.get('RAW_COMPARISON_RESULTS_S3_PATH'),
                    comparison_results_s3_path=system_config.get('COMPARISON_RESULTS_S3_PATH'))
            }

        elif action == "sql_status":
            res = {'status': sql_status(sql_id)}
        elif action == "run_glue_crawler":
            res = {'status': run_glue_crawler(system_config.get('CRAWLER_NAME'))}
        else:
            raise ValueError("Invalid Task: " + action)
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        raise
    print(res)
    return res


def populate_comparison_results(script_s3_path, action, cluster_identifier, redshift_iam_role, bucket_name, db, user,
                                what_if_timestamp, raw_comparison_results_s3_path, comparison_results_s3_path):
    return run_sql_script_from_s3(script_s3_path=script_s3_path,
                                  action=action,
                                  cluster_identifier=cluster_identifier,
                                  redshift_iam_role=redshift_iam_role,
                                  bucket_name=bucket_name,
                                  db=db,
                                  user=user,
                                  what_if_timestamp=what_if_timestamp,
                                  raw_comparison_results_s3_path=raw_comparison_results_s3_path,
                                  comparison_results_s3_path=comparison_results_s3_path)



def s3_put(script_s3_path, object):
    bucket, key = script_s3_path.replace("s3://", "").split("/", 1)
    boto3.client('s3').put_object(Bucket=bucket, Key=key, Body=object)

def gather_comparison_stats(script_s3_path, action, cluster_identifier, redshift_iam_role, bucket_name, db,
                            user, run_type, what_if_timestamp, comparison_stats_s3_path, external_schema_script,
                            query_label_prefix,node_type,number_of_nodes,region,cluster_config_s3_path):
    config=f'cluster_identifier,node_type,number_of_nodes,region\n{cluster_identifier},{node_type},{number_of_nodes},{region}'
    s3_put(cluster_config_s3_path+'/'+cluster_identifier+'.csv', config)
    try:
        run_sql(clusterid=cluster_identifier,
                db=db,
                user=user,
                script=external_schema_script,
                with_event=False,
                run_type='sync')
    except Exception as e:
        if "already exists" not in str(e):
            raise
    return run_sql_script_from_s3(script_s3_path=script_s3_path,
                                  action=action,
                                  cluster_identifier=cluster_identifier,
                                  redshift_iam_role=redshift_iam_role,
                                  bucket_name=bucket_name,
                                  db=db,
                                  user=user,
                                  run_type=run_type,
                                  what_if_timestamp=what_if_timestamp,
                                  comparison_stats_s3_path=comparison_stats_s3_path,
                                  query_label_prefix=query_label_prefix)


def pause_cluster(client, cluster_identifier, redshift_cluster_index, auto_pause):
    if auto_pause and redshift_cluster_index > 0:
        try:
            client.pause_cluster(ClusterIdentifier=cluster_identifier)
        except be.ClientError as e:
            if e.response['Error']['Code'] == 'InvalidClusterState':
                print(e.response['Error']['Code'])
            else:
                raise
        return "initiated"
    else:
        return "auto_pause config is false"

def cluster_status(client, clusterid):
    try:
        desc = client.describe_clusters(ClusterIdentifier=clusterid)['Clusters'][0]
        if isinstance(desc, dict):
            status = desc.get('ClusterStatus') + desc.get('ClusterAvailabilityStatus') + (
                desc.get('RestoreStatus').get('Status') if desc.get('RestoreStatus') else "")
        else:
            status = 'Unavailable'
    except be.ClientError as e:
        msg = e.response['Error']['Code']
        if msg == 'ClusterNotFound':
            status = 'nonExistent'
        else:
            raise
    return status



def get_cluster_identifier(client, config, redshift_configurations, cluster_identifier_prefix):
    if redshift_configurations.get('USER_FRIENDLY_NAME_SUFFIX') is None or redshift_configurations.get(
            'USER_FRIENDLY_NAME_SUFFIX') == 'N/A':
        if redshift_configurations.get('WLM_CONFIG_S3_PATH') is None or redshift_configurations.get(
                'WLM_CONFIG_S3_PATH') == 'N/A':
            wlm_name = ""
        else:
            wlm_name = redshift_configurations.get('WLM_CONFIG_S3_PATH').replace("s3://", "").replace("/", "").replace(
                ".json", "")
            wlm_name = re.sub('[^A-Za-z0-9]+', '', wlm_name)
        cluster_suffix = redshift_configurations.get('NODE_TYPE') + "-" + redshift_configurations.get(
            'NUMBER_OF_NODES') + wlm_name
        cluster_suffix = cluster_suffix.replace(".", "-")
    else:
        cluster_suffix = redshift_configurations.get('USER_FRIENDLY_NAME_SUFFIX')
    return (cluster_identifier_prefix + "-" + cluster_suffix).lower()[0:63]


def update_wlm_config(client, cluster_identifier, wlm_config_s3_path):
    if wlm_config_s3_path is None or wlm_config_s3_path == "N/A":
        return "N/A"
    else:
        wlm_config = get_json_config_from_s3(wlm_config_s3_path)
        print("Changing {} parameter group wlm : {}".format(cluster_identifier, wlm_config))
        client.modify_cluster_parameter_group(
            ParameterGroupName=cluster_identifier,
            Parameters=[
                {
                    'ParameterName': 'wlm_json_configuration',
                    'ParameterValue': json.dumps(wlm_config),
                    'ApplyType': 'dynamic',
                    'IsModifiable': True
                },
            ])
    return "initiated"
    
## Added to check for clusters in pending reboot after wlm change ##
def check_pending_reboot_status(client,cluster_identifier):
    try:
        cluster_desc = client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        desc_paramgroup_status = cluster_desc['ClusterParameterGroups'][0]['ParameterApplyStatus']
        status = cluster_desc.get('ClusterStatus') + cluster_desc.get('ClusterAvailabilityStatus') + desc_paramgroup_status
        if desc_paramgroup_status == 'pending-reboot':
                print('Cluster {} needs to be rebooted to apply the WLM config changes'.format(cluster_identifier))
                client.reboot_cluster(ClusterIdentifier=cluster_identifier)
    except Exception as err:
        print(err)
        status = 'availableAvailablein-sync'
    return status

def get_json_config_from_s3(script_s3_path):
    bucket, key = script_s3_path.replace("s3://", "").split("/", 1)
    obj = boto3.client('s3').get_object(Bucket=bucket, Key=key)
    return json.loads(obj['Body'].read().decode('utf-8'))


def create_parameter_group(client, parameter_group_name):
    try:
        client.create_cluster_parameter_group(
            ParameterGroupName=parameter_group_name,
            ParameterGroupFamily='redshift-1.0',
            Description='redshift cluster parameter group'
        )
    except be.ClientError as e:
        if e.response['Error']['Code'] == 'ClusterParameterGroupAlreadyExists':
            print(e.response['Error']['Code'])
        else:
            raise
    return 'initiated'


def parameter_group_status(client, parameter_group_name):
    parameter_group = client.describe_cluster_parameters(ParameterGroupName=parameter_group_name)
    return parameter_group


def update_parameter_group(client, parameter_group_name, parameter_group_config_s3_path):
    target_parameter_group = client.describe_cluster_parameters(ParameterGroupName=parameter_group_name)["Parameters"]
    target_parameters = {}
    for i in target_parameter_group:
        target_parameters[i['ParameterName']] = i['ParameterValue']
    source_parameter_group = get_json_config_from_s3(parameter_group_config_s3_path)["Parameters"]
    modified_parameter_group = []
    for i in source_parameter_group:
        source_parameter_value = i['ParameterValue'].replace(" ", "")
        target_parameter_value = target_parameters[i['ParameterName']].replace(" ", "")
        if source_parameter_value != target_parameter_value:
            modified_parameter_group.append(i)
    if modified_parameter_group:
        client.modify_cluster_parameter_group(
            ParameterGroupName=parameter_group_name,
            Parameters=modified_parameter_group)
    return "Initiated"


def classic_resize_cluster(client, clusterid, node_type, number_of_nodes):
    client.resize_cluster(ClusterIdentifier=clusterid, NodeType=node_type, NumberOfNodes=int(number_of_nodes),
                          ClusterType='single-node' if int(number_of_nodes) == 1 else 'multi-node', Classic=True)
    return "Initiated"


def create_cluster(client, cluster_identifier, snapshot_id, redshift_iam_role, parameter_group_name, subnet_group,
                   security_group_id, snapshot_account_id, node_type, number_of_nodes, master_user_name, database_name,
                   port, publicly_accessible, secrets_manager_arn):
    try:
        if snapshot_id is None or snapshot_id == "N/A":
            master_user_secret = json.loads(
                boto3.client('secretsmanager').get_secret_value(SecretId=secrets_manager_arn).get('SecretString'))
            master_user_password = master_user_secret.get('password')
            client.create_cluster(DBName=database_name,
                                  ClusterIdentifier=cluster_identifier,
                                  ClusterType='single-node' if int(number_of_nodes) == 1 else 'multi-node',
                                  NodeType=node_type,
                                  MasterUsername=master_user_name,
                                  MasterUserPassword=master_user_password,
                                  VpcSecurityGroupIds=[security_group_id],
                                  ClusterSubnetGroupName=subnet_group,
                                  ClusterParameterGroupName=parameter_group_name,
                                  Port=port,
                                  NumberOfNodes=int(number_of_nodes),
                                  PubliclyAccessible=publicly_accessible,
                                  IamRoles=[redshift_iam_role])
        else:
            if snapshot_account_id is None or snapshot_account_id == "N/A":
                snapshot_account_id = boto3.client('sts').get_caller_identity()['Account']

            client.restore_from_cluster_snapshot(NumberOfNodes=int(number_of_nodes),
                                                 NodeType=node_type,
                                                 ClusterIdentifier=cluster_identifier,
                                                 SnapshotIdentifier=snapshot_id,
                                                 OwnerAccount=snapshot_account_id,
                                                 Port=port,
                                                 ClusterSubnetGroupName=subnet_group,
                                                 PubliclyAccessible=publicly_accessible,
                                                 ClusterParameterGroupName=parameter_group_name,
                                                 VpcSecurityGroupIds=[security_group_id],
                                                 IamRoles=[redshift_iam_role])
        status = 'Initiated'
    except be.ClientError as e:
        msg = e.response['Error']['Code']
        if msg == 'ClusterAlreadyExists':
            status = msg
        elif msg == 'InvalidParameterValue':
            source_node_type, source_number_of_nodes = get_source_cluster_config(client, snapshot_id)
            client.restore_from_cluster_snapshot(NumberOfNodes=source_number_of_nodes,
                                                 NodeType=source_node_type,
                                                 ClusterIdentifier=cluster_identifier,
                                                 SnapshotIdentifier=snapshot_id,
                                                 OwnerAccount=snapshot_account_id,
                                                 Port=port,
                                                 ClusterSubnetGroupName=subnet_group,
                                                 PubliclyAccessible=publicly_accessible,
                                                 ClusterParameterGroupName=parameter_group_name,
                                                 VpcSecurityGroupIds=[security_group_id],
                                                 IamRoles=[redshift_iam_role])
            status = 'NeedClassicResize'
        else:
            raise
    return status


def get_source_cluster_config(client, snapshot_id):
    resp = client.describe_cluster_snapshots(SnapshotIdentifier=snapshot_id)
    node_type = resp['Snapshots'][0]['NodeType']
    number_of_nodes = resp['Snapshots'][0]['NumberOfNodes']
    return (node_type, number_of_nodes)


def run_sql_script_from_s3(script_s3_path, action, cluster_identifier, redshift_iam_role, bucket_name, db,
                           user, run_type='async', result_cache='true', with_event=False, what_if_timestamp=None,
                           comparison_stats_s3_path=None, comparison_results_s3_path=None,
                           raw_comparison_results_s3_path=None, query_label_prefix=None):
    if script_s3_path is None or script_s3_path == "N/A":
        return "N/A"
    else:
        bucket, key = script_s3_path.replace("s3://", "").split("/", 1)
        obj = boto3.client('s3').get_object(Bucket=bucket, Key=key)
        script = obj['Body'].read().decode('utf-8')
        script = script.format(redshift_iam_role=redshift_iam_role,
                               bucket_name=bucket_name,
                               cluster_identifier=cluster_identifier,
                               what_if_timestamp=what_if_timestamp,
                               comparison_stats_s3_path=comparison_stats_s3_path,
                               comparison_results_s3_path=comparison_results_s3_path,
                               raw_comparison_results_s3_path=raw_comparison_results_s3_path,
                               query_label_prefix=query_label_prefix)
        query_group_statement = "set query_group to '" + action + "';\n"
        result_cache_statement = "set enable_result_cache_for_session to " + result_cache + "; \n"
        script = query_group_statement + result_cache_statement + script
        sql_id = run_sql(cluster_identifier, db, user, script, with_event, run_type)
        return sql_id


def run_sql(clusterid, db, user, script, with_event, run_type):
    res = boto3.client("redshift-data").execute_statement(Database=db, DbUser=user, Sql=script,
                                                          ClusterIdentifier=clusterid, WithEvent=with_event)
    query_id = res["Id"]
    done = False
    while not done:
        status = sql_status(query_id)
        if run_type == 'async':
            break
        elif status == "FINISHED":
            break
    return query_id




def sql_status(query_id):
    if query_id == "N/A":
        return "FINISHED"
    res = boto3.client("redshift-data").describe_statement(Id=query_id)
    status = res["Status"]
    if status == "FAILED":
        print(res)
        raise Exception(res["Error"])
    return status.strip('"')


def run_redshift_performance_test(client, cluster_identifier, bucket_name, performance_test_bootstrap_script,
                                  performance_test_python_script,
                                  sql_script_s3_path, number_of_parallel_sessions_list, job_definition, job_queue,
                                  redshift_iam_role, redshift_user_name, db,
                                  disable_result_cache, default_output_limit, max_number_of_queries,
                                  max_parallel_sessions, query_label_prefix):
    if sql_script_s3_path is None or sql_script_s3_path == "N/A":
        return "N/A"
    else:
        desc = client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        cluster_endpoint = desc.get('Endpoint').get('Address') + ":" + str(desc.get('Endpoint').get('Port')) + "/" + db
        response = boto3.client('batch').submit_job(jobName='AmazonRedshiftPerformanceTesting',
                                                    jobQueue=job_queue,
                                                    jobDefinition=job_definition,
                                                    containerOverrides={
                                                        "command": ["sh", "-c",
                                                                    "yum install -y awscli; aws s3 cp $BOOTSTRAP_SCRIPT ./bootstrap.sh; sh ./bootstrap.sh"],
                                                        "environment": [
                                                            {"name": "BOOTSTRAP_SCRIPT",
                                                             "value": performance_test_bootstrap_script},
                                                            {"name": "BUCKET_NAME", "value": bucket_name},
                                                            {"name": "PYTHON_SCRIPT",
                                                             "value": performance_test_python_script},
                                                            {"name": "REDSHIFT_CLUSTER_ENDPOINT",
                                                             "value": cluster_endpoint},
                                                            {"name": "REDSHIFT_IAM_ROLE", "value": redshift_iam_role},
                                                            {"name": "REDSHIFT_USER_NAME", "value": redshift_user_name},
                                                            {"name": "SQL_SCRIPT_S3_PATH", "value": sql_script_s3_path},
                                                            {"name": "NUMBER_OF_PARALLEL_SESSIONS_LIST",
                                                             "value": number_of_parallel_sessions_list},
                                                            {"name": "DISABLE_RESULT_CACHE",
                                                             "value": disable_result_cache},
                                                            {"name": "DEFAULT_OUTPUT_LIMIT",
                                                             "value": default_output_limit},
                                                            {"name": "MAX_NUMBER_OF_QUERIES",
                                                             "value": max_number_of_queries},
                                                            {"name": "MAX_PARALLEL_SESSIONS",
                                                             "value": max_parallel_sessions},
                                                            {"name": "QUERY_LABEL_PREFIX", "value": query_label_prefix}
                                                        ]
                                                    })
    return response['jobId']


def get_workload_location(extract_s3_path):
    bucket, key = extract_s3_path.replace("s3://", "").split("/", 1)
    response = boto3.client('s3').list_objects_v2(Bucket=bucket, Prefix=key)
    if response.get('Contents'):
        key = response.get('Contents')[0].get('Key').split('/')[2]
        return extract_s3_path + key
    else:
        return None


def run_extract(what_if_timestamp, simple_replay_log_location,
                simple_replay_extract_start_time, simple_replay_extract_end_time,
                simple_replay_extract_overwrite_s3_path,
                bucket_name, redshift_user_name,
                extract_prefix, script_prefix, extract_bootstrap_script, job_definition, job_queue):
    if simple_replay_log_location is None or simple_replay_log_location == "N/A":
        return "N/A"
    else:
        if simple_replay_extract_overwrite_s3_path is None:
            simple_replay_extract_overwrite_s3_path="N/A"

        response = boto3.client('batch').submit_job(jobName='AmazonRedshiftExtract',
                                                    jobQueue=job_queue,
                                                    jobDefinition=job_definition,
                                                    containerOverrides={
                                                        "command": ["sh", "-c",
                                                                    "yum install -y awscli; aws s3 cp $BOOTSTRAP_SCRIPT ./bootstrap.sh; sh ./bootstrap.sh"],
                                                        "environment": [
                                                            {"name": "BOOTSTRAP_SCRIPT",
                                                             "value": extract_bootstrap_script},
                                                            {"name": "BUCKET_NAME", "value": bucket_name},
                                                            {"name": "SIMPLE_REPLAY_EXTRACT_OVERWRITE_S3_PATH",
                                                             "value": simple_replay_extract_overwrite_s3_path},
                                                            {"name": "SIMPLE_REPLAY_LOG_LOCATION",
                                                             "value": simple_replay_log_location},
                                                            {"name": "REDSHIFT_USER_NAME", "value": redshift_user_name},
                                                            {"name": "WHAT_IF_TIMESTAMP", "value": what_if_timestamp},
                                                            {"name": "SIMPLE_REPLAY_EXTRACT_START_TIME",
                                                             "value": simple_replay_extract_start_time},
                                                            {"name": "SIMPLE_REPLAY_EXTRACT_END_TIME",
                                                             "value": simple_replay_extract_end_time},
                                                            {"name": "EXTRACT_PREFIX", "value": extract_prefix},
                                                            {"name": "SCRIPT_PREFIX", "value": script_prefix}
                                                        ]
                                                    })
        return response['jobId']


def run_replay(client, what_if_timestamp, cluster_identifier, extract_s3_path, simple_replay_log_location,
               simple_replay_overwrite_s3_path, bucket_name, redshift_user_name,
               redshift_iam_role, db, extract_prefix, replay_prefix,script_prefix, snapshot_account_id,
               replay_bootstrap_script, job_definition, job_queue):
    if simple_replay_log_location is None or simple_replay_log_location == "N/A":
        return "N/A"
    else:
        if simple_replay_overwrite_s3_path is None:
            simple_replay_overwrite_s3_path="N/A"
        desc = client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        cluster_endpoint = desc.get('Endpoint').get('Address') + ":" + str(desc.get('Endpoint').get('Port')) + "/" + db
        workload_location = get_workload_location(extract_s3_path)
        response = boto3.client('batch').submit_job(jobName='AmazonRedshiftReplay',
                                                    jobQueue=job_queue,
                                                    jobDefinition=job_definition,
                                                    containerOverrides={
                                                        "command": ["sh", "-c",
                                                                    "yum install -y awscli; aws s3 cp $BOOTSTRAP_SCRIPT ./bootstrap.sh; sh ./bootstrap.sh"],
                                                        "environment": [
                                                            {"name": "BOOTSTRAP_SCRIPT",
                                                             "value": replay_bootstrap_script},
                                                            {"name": "WHAT_IF_TIMESTAMP", "value": what_if_timestamp},
                                                            {"name": "CLUSTER_IDENTIFIER", "value": cluster_identifier},
                                                            {"name": "CLUSTER_ENDPOINT", "value": cluster_endpoint},
                                                            {"name": "WORKLOAD_LOCATION", "value": workload_location},
                                                            {"name": "SIMPLE_REPLAY_OVERWRITE_S3_PATH",
                                                             "value": simple_replay_overwrite_s3_path},
                                                            {"name": "SIMPLE_REPLAY_LOG_LOCATION",
                                                             "value": simple_replay_log_location},
                                                            {"name": "BUCKET_NAME", "value": bucket_name},
                                                            {"name": "REDSHIFT_USER_NAME", "value": redshift_user_name},
                                                            {"name": "REDSHIFT_IAM_ROLE", "value": redshift_iam_role},
                                                            {"name": "EXTRACT_PREFIX", "value": extract_prefix},
                                                            {"name": "REPLAY_PREFIX", "value": replay_prefix},
                                                            {"name": "SCRIPT_PREFIX", "value": script_prefix},
                                                            {"name": "SNAPSHOT_ACCOUNT_ID", "value": snapshot_account_id}
                                                        ]
                                                    })

        return response['jobId']


def batch_job_status(job_id, extract_s3_path=None):
    if job_id == "N/A":
        return "FINISHED"
    else:
        job_stats = boto3.client('batch').describe_jobs(jobs=[job_id]).get('jobs')[0]
        if job_stats.get('status') == "FAILED":
            raise Exception('Error:' + str(job_stats))
        elif job_stats.get('status') == "SUCCEEDED":
            return "FINISHED"
        else:
            return job_stats.get('status')

def run_glue_crawler(crawler_name):
    try:
        response = boto3.client('glue').start_crawler(Name=crawler_name)
        return "initiated"
    except be.ClientError as e:
        raise Exception("run_glue_crawler: " + e.__str__())
    except Exception as e:
        raise Exception("run_glue_crawler: " + e.__str__())
