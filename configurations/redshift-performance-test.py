import json
import boto3
import psycopg2
import time
import pandas
from sqlalchemy import create_engine
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from urllib.parse import quote_plus as urlquote
import urllib
import re
import os

SQL_SCRIPT_S3_PATH=os.environ['SQL_SCRIPT_S3_PATH']
REDSHIFT_CLUSTER_ENDPOINT=os.environ['REDSHIFT_CLUSTER_ENDPOINT']
REDSHIFT_IAM_ROLE=os.environ['REDSHIFT_IAM_ROLE']
BUCKET_NAME=os.environ['SQL_SCRIPT_S3_PATH']
REDSHIFT_USER_NAME=os.environ['REDSHIFT_USER_NAME']
NUMBER_OF_PARALLEL_SESSIONS_LIST=os.environ['NUMBER_OF_PARALLEL_SESSIONS_LIST']
DISABLE_RESULT_CACHE=os.environ['DISABLE_RESULT_CACHE']
DEFAULT_OUTPUT_LIMIT=os.environ['DEFAULT_OUTPUT_LIMIT']
MAX_NUMBER_OF_QUERIES=os.environ['MAX_NUMBER_OF_QUERIES']
MAX_PARALLEL_SESSIONS=os.environ['MAX_PARALLEL_SESSIONS']
QUERY_LABEL_PREFIX=os.environ['QUERY_LABEL_PREFIX']


def connect_to_redshift(host,username):
    client = boto3.client('redshift')
    cluster_creds = client.get_cluster_credentials(DbUser=username,
                                                   DbName=REDSHIFT_CLUSTER_ENDPOINT.split('/')[1],
                                                   ClusterIdentifier=REDSHIFT_CLUSTER_ENDPOINT.split('.')[0])


    connection_string='postgresql://'+ urlquote(cluster_creds['DbUser']) + ':'+ urlquote(cluster_creds['DbPassword']) + '@'+ REDSHIFT_CLUSTER_ENDPOINT
    return create_engine(connection_string,pool_size=0, max_overflow=-1)

def get_json_config_from_s3(script_s3_path):
    bucket, key = script_s3_path.replace("s3://", "").split("/", 1)
    obj = boto3.client('s3').get_object(Bucket=bucket, Key=key)
    return json.loads(obj['Body'].read().decode('utf-8'))


def get_sql_scripts_from_s3():

    bucket, key = SQL_SCRIPT_S3_PATH.replace("s3://", "").split("/", 1)
    obj = boto3.client('s3').get_object(Bucket=bucket, Key=key)
    script = obj['Body'].read().decode('utf-8')
    script = script.format(redshift_iam_role=REDSHIFT_IAM_ROLE, bucket_name=BUCKET_NAME)
    split_scripts = script.split(';')[:-1]
    if len(split_scripts)>int(MAX_NUMBER_OF_QUERIES):
        split_scripts=split_scripts[0:int(MAX_NUMBER_OF_QUERIES)]
    return split_scripts


def get_sql(engine, number_of_parallel_sessions):
    sql_script = ""

    pattern = re.compile(r'limit[\s|\t|\n]+[\d]+[\s]*$', re.IGNORECASE)
    for query in get_sql_scripts_from_s3():
        if not re.search(pattern, query):
            query += " limit " + DEFAULT_OUTPUT_LIMIT
        sql_script+=query + ";\n"

    if DISABLE_RESULT_CACHE=='true':
        sql_script = "set enable_result_cache_for_session to false;\n" + sql_script

    sql_script = "set query_group to '" + QUERY_LABEL_PREFIX + str(number_of_parallel_sessions) + "';\n" + sql_script

    df = pandas.read_sql(text(sql_script), engine)
    return df



def run_concurrency_test(number_of_parallel_sessions):
    engine=connect_to_redshift(REDSHIFT_CLUSTER_ENDPOINT,REDSHIFT_USER_NAME)
    start_time = time.time()
    try:
        with ThreadPoolExecutor(max_workers=number_of_parallel_sessions) as executor:
            futures = []
            for _ in range(number_of_parallel_sessions):
                futures.append(executor.submit(
                    get_sql, engine, number_of_parallel_sessions))
            for future in as_completed(futures):
                rs = future.result()

    except Exception as e:
        raise e
    elapsed_time_in_secs = (time.time() - start_time)
    print("--- %s seconds ---" % elapsed_time_in_secs)
    return elapsed_time_in_secs

print(f'script:{SQL_SCRIPT_S3_PATH}, cluster:{REDSHIFT_CLUSTER_ENDPOINT},role:{REDSHIFT_IAM_ROLE},bucket:{BUCKET_NAME},user:{REDSHIFT_USER_NAME},sessions:{NUMBER_OF_PARALLEL_SESSIONS_LIST}')
for sessions in NUMBER_OF_PARALLEL_SESSIONS_LIST.split(','):
    number_of_parallel_sessions=int(sessions)
    if number_of_parallel_sessions <= int(MAX_PARALLEL_SESSIONS):
        print(f'running {number_of_parallel_sessions} parallel threads ..')
        run_concurrency_test(number_of_parallel_sessions)
    else:
        print(f'parallel sessions {number_of_parallel_sessions} exceeds maximum allowed {MAX_PARALLEL_SESSIONS} ..')
