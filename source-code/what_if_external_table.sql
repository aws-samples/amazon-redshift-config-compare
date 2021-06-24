create external table redshift_config.query_stats (
queue varchar(64),
username varchar(128),
cc_scaling int,
aborted int,
queue_time bigint,
compile_time bigint,
exec_time bigint,
total_query_time bigint,
userid int,
query int,
query_label varchar(100),
xid bigint,
pid int,
service_class int,
starttime timestamp,
endtime timestamp,
tables_scanned varchar(65535),
querytxt varchar(65535),
query_hash varchar(100)
)
partitioned by (whatif_timestamp varchar(100), cluster_identifier varchar(100))
row format delimited
fields terminated by ','
stored as parquet
location 's3://{bucket_name}/query_stats/';
