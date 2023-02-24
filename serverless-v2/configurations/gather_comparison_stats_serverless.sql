unload ($$
select a.*,Trim(u.usename) as username from sys_query_history a , pg_user u
where a.user_id = u.usesysid
and a.start_time > to_timestamp('{what_if_timestamp}','YYYY-MM-DD-HH24-MI-SS')
$$) to '{comparison_stats_s3_path}/{what_if_timestamp}/{cluster_identifier}/'
FORMAT AS PARQUET ALLOWOVERWRITE iam_role '{redshift_iam_role}';


create or replace view public.redshift_config_comparison_raw as
SELECT
partition_1 cluster_identifier,
       SHA2(split_part(query_text,'}}',2),256) as query_hash,
       round( execution_time   / ( 1000 * 1000 ) ,2) exec_time_seconds,
       round( elapsed_time   / ( 1000 * 1000 ) ,2) elasped_time_seconds,
       round( queue_time   / ( 1000 * 1000 ) ,2) queue_time_seconds,
       user_id,
       username,
       database_name,
       queue_time,
       execution_time,
       elapsed_time,
       query_id,
       trim(query_label::varchar) query_label,
       transaction_id,
       session_id,
       result_cache_hit,
       start_time,
       end_time,
       error_message,
       query_text ,
       status,
       partition_0::varchar analysis_timestamp
FROM redshift_config_comparison.comparison_stats q
where  partition_0 = (SELECT
                                 MAX(partition_0)
                             FROM
                                 redshift_config_comparison.comparison_stats)
         --AND (query_label like '{query_label_prefix}%' or  q.querytxt LIKE '%Replay source file%')
         AND q.query_text LIKE '%replay_start%'
with no schema binding;


create or replace view public.redshift_config_comparison_aggregate as
SELECT  case when query_label like '{query_label_prefix}%' then query_label else 'simple-replay' end test_type
       ,cluster_identifier
       ,username
      , ROUND(SUM(r.elapsed_time::NUMERIC) / ( 1000 * 1000 ) ,2) total_query_time_seconds
      , ROUND(AVG(r.elapsed_time::NUMERIC) / ( 1000 * 1000 ) ,2) mean_query_time_seconds
       ,Percentile_cont(1.0) within group(ORDER BY elapsed_time) AS max_query_time_seconds
      , ROUND(( PERCENTILE_CONT(0.50) WITHIN GROUP( ORDER BY elapsed_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct50_query_time_seconds
      , ROUND(( PERCENTILE_CONT(0.75) WITHIN GROUP( ORDER BY elapsed_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct75_query_time_seconds
      , ROUND(( PERCENTILE_CONT(0.90) WITHIN GROUP( ORDER BY elapsed_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct90_query_time_seconds
      , ROUND(( PERCENTILE_CONT(0.95) WITHIN GROUP( ORDER BY elapsed_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct95_query_time_seconds
      ,ROUND(( PERCENTILE_CONT(0.99) WITHIN GROUP( ORDER BY elapsed_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct99_query_time_seconds
      ,SUM(1) count_queries
      , analysis_timestamp
FROM public.redshift_config_comparison_raw r
group by test_type,
       cluster_identifier,
       username,
       analysis_timestamp
with no schema binding
;

drop view if exists public.redshift_config_comparison_results cascade;

create or replace view public.redshift_config_comparison_results as
with agg_data as (SELECT test_type,
       cluster_identifier,
       username,
       total_query_time_seconds,
       max(total_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following) max_total_query_time_seconds,
       mean_query_time_seconds,
       max(mean_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_mean_query_time_seconds,
       pct50_query_time_seconds,
       max(pct50_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_pct50_query_time_seconds,
       max_query_time_seconds,
       max(max_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_max_query_time_seconds,
       pct75_query_time_seconds,
       max(pct75_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_pct75_query_time_seconds,
       pct90_query_time_seconds,
       max(pct90_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_pct90_query_time_seconds,
       pct95_query_time_seconds,
       max(pct95_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_pct95_query_time_seconds,
       pct99_query_time_seconds,
       max(pct99_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_pct99_query_time_seconds,
       count_queries,
       analysis_timestamp
FROM public.redshift_config_comparison_aggregate)
select test_type,
 cluster_identifier,
       username,
       total_query_time_seconds,
      round(((max_total_query_time_seconds-total_query_time_seconds)/case when total_query_time_seconds=0 then 1 else total_query_time_seconds end)*100)||'%'   improvement_total_query_time,
       mean_query_time_seconds,
	   round(((max_mean_query_time_seconds-mean_query_time_seconds)/case when mean_query_time_seconds=0 then 1 else mean_query_time_seconds end)*100)||'%'   improvement_mean_query_time,
       pct50_query_time_seconds,
       round(((max_pct50_query_time_seconds-pct50_query_time_seconds)/case when pct50_query_time_seconds=0 then 1 else pct50_query_time_seconds end)*100)||'%'   improvement_pct50_query_time,
       max_query_time_seconds,
       round(((max_max_query_time_seconds-max_query_time_seconds)/case when max_query_time_seconds=0 then 1 else max_query_time_seconds end)*100)||'%'   improvement_max_query_time,
       pct75_query_time_seconds,
        round(((max_pct75_query_time_seconds-pct75_query_time_seconds)/case when pct75_query_time_seconds=0 then 1 else pct75_query_time_seconds end)*100)||'%'   improvement_pct75_query_time,
       pct90_query_time_seconds,
        round(((max_pct90_query_time_seconds-pct90_query_time_seconds)/case when pct90_query_time_seconds=0 then 1 else pct90_query_time_seconds end)*100)||'%'   improvement_pct90_query_time,
       pct95_query_time_seconds,
      round(((max_pct95_query_time_seconds-pct95_query_time_seconds)/case when pct95_query_time_seconds=0 then 1 else pct95_query_time_seconds end)*100)||'%'   improvement_pct95_query_time,
      pct99_query_time_seconds,
       round(((max_pct99_query_time_seconds-pct99_query_time_seconds)/case when pct99_query_time_seconds=0 then 1 else pct99_query_time_seconds end)*100)||'%'   improvement_pct99_query_time,
      count_queries,
      analysis_timestamp
 from agg_data
with no schema binding;