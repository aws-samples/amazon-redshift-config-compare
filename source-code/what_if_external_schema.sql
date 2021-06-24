drop schema if exists redshift_config;

create external schema redshift_config from data catalog database 'redshift_config' iam_role '{redshift_iam_role}' create external database if not exists;
