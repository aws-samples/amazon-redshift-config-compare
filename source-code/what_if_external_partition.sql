alter table redshift_config.query_stats add partition(what_if_timestamp='{what_if_timestamp}',cluster_identifier='{cluster_identifier}')
location 's3://{bucket_name}/query_stats/{what_if_timestamp}/{cluster_identifier}/';
