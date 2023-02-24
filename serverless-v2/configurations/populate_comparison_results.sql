unload ($$
select * from public.redshift_config_comparison_raw
$$) to '{raw_comparison_results_s3_path}/{what_if_timestamp}/'
FORMAT AS CSV HEADER ALLOWOVERWRITE iam_role '{redshift_iam_role}';


unload ($$
select * from public.redshift_config_comparison_results
$$) to '{comparison_results_s3_path}/{what_if_timestamp}/'
parallel off FORMAT AS CSV HEADER ALLOWOVERWRITE iam_role '{redshift_iam_role}';
