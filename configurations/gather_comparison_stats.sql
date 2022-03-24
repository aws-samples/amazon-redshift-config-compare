
drop view if exists public.redshift_detailed_query_stats cascade;

CREATE OR replace VIEW public.redshift_detailed_query_stats
AS
  WITH queries
       AS (SELECT query,
                  Listagg(DISTINCT schemaname
                                   ||'.'
                                   ||table_name, ',')
                    within GROUP(ORDER BY table_name) tables_scanned
           FROM   (WITH scan_delete_insert
                        AS (SELECT 'scan'                query_type,
                                   query,
                                   Lpad(segment, 3, '0') segment,
                                   tbl
                            FROM   stl_scan
                            WHERE  userid > 1
                                   AND perm_table_name != 'Internal Worktable'
                                   AND tbl <> 0
                            UNION ALL
                            SELECT 'delete'              query_type,
                                   query,
                                   Lpad(segment, 3, '0') segment,
                                   tbl
                            FROM   stl_delete
                            WHERE  userid > 1
                                   AND tbl <> 0
                            UNION ALL
                            SELECT 'insert'              query_type,
                                   query,
                                   Lpad(segment, 3, '0') segment,
                                   tbl
                            FROM   stl_insert
                            WHERE  userid > 1
                                   AND tbl <> 0)
                   SELECT sdi.query_type,
                          sdi.query,
                          sdi.segment,
                          sdi.tbl,
                          Trim(n.nspname) AS schemaname,
                          Trim(c.relname) table_name
                    FROM   scan_delete_insert sdi
                           join pg_class c
                             ON c.oid = sdi.tbl
                           join pg_namespace n
                             ON n.oid = c.relnamespace)
           GROUP  BY query),
       compiles
       AS (SELECT query,
                  SUM(Datediff (microsecond, q.starttime, q.endtime))
                  compile_time
           FROM   svl_compile q
           WHERE  COMPILE = 1
           GROUP  BY query)
  SELECT nvl(s.name,'Result Cache') as queue,
         Trim(u.usename)                                   AS username,
         CASE
           WHEN q.concurrency_scaling_status = 1 THEN 1
           ELSE 0
         END                                               AS cc_scaling,
         q.aborted,
         w.total_queue_time                                queue_time,
         Nvl(ct.compile_time, 0)                     compile_time,
         w.total_exec_time - Nvl(ct.compile_time, 0) exec_time,
         Datediff(microsecond, q.starttime, q.endtime)     AS total_query_time,
         q.userid,
         q.query,
         q.label                                           query_label,
         q.xid,
         q.pid,
         w.service_class,
         q.starttime,
         q.endtime,
         tables_scanned,
         Trim(q.querytxt)                                  querytxt,
         SHA2(q.querytxt,256) query_hash
  FROM   stl_query q
         join stl_wlm_query w USING (userid, query)
         join pg_user u
           ON u.usesysid = q.userid
         left join stv_wlm_service_class_config s
           ON w.service_class = s.service_class
         left outer join queries
                      ON queries.query = q.query
         left outer join compiles ct
                      ON w.query = ct.query
  WHERE  q.userid > 1
         AND w.service_class > 5;


drop view if exists public.redshift_config_comparison_raw cascade;


create or replace view public.redshift_config_comparison_raw as
SELECT
partition_1 cluster_identifier,
       SHA2(split_part(querytxt,'}',2),256) as query_hash,
       round( exec_time   / ( 1000 * 1000 ) ,2) exec_time_seconds,
       round( total_query_time   / ( 1000 * 1000 ) ,2) total_query_time_seconds,
       round( compile_time   / ( 1000 * 1000 ) ,2) compile_time_seconds,
       round( queue_time   / ( 1000 * 1000 ) ,2) queue_time_seconds,
       queue,
       username,
       cc_scaling,
       aborted,
       queue_time,
       compile_time,
       exec_time,
       total_query_time,
       userid,
       query,
       trim(query_label::varchar) query_label,
       xid,
       pid,
       service_class,
       starttime,
       endtime,
       tables_scanned,
       querytxt ,
       partition_0::varchar analysis_timestamp
FROM redshift_config_comparison.comparison_stats q
where  partition_0 = (SELECT
                                 MAX(partition_0)
                             FROM
                                 redshift_config_comparison.comparison_stats)
         --AND (query_label like '{query_label_prefix}%' or  q.querytxt LIKE '%Replay source file%')
         AND q.querytxt LIKE '%replay_start%'
with no schema binding;



drop view if exists public.redshift_config_comparison_aggregate cascade;


create or replace view public.redshift_config_comparison_aggregate as
SELECT  case when query_label like '{query_label_prefix}%' then query_label else 'simple-replay' end test_type
       ,cluster_identifier
       ,queue
       ,username
      , ROUND(SUM(r.total_query_time::NUMERIC) / ( 1000 * 1000 ) ,2) total_query_time_seconds
      , ROUND(AVG(r.total_query_time::NUMERIC) / ( 1000 * 1000 ) ,2) mean_query_time_seconds
      , ROUND(( PERCENTILE_CONT(0.50) WITHIN GROUP( ORDER BY total_query_time) )::NUMERIC / ( 1000 * 1000 ),3) AS median_query_time_seconds
      , ROUND(MAX(total_query_time)::NUMERIC / ( 1000 * 1000 ),2) max_query_time_seconds
      , ROUND(( PERCENTILE_CONT(0.75) WITHIN GROUP( ORDER BY total_query_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct75_query_time_seconds
      , ROUND(( PERCENTILE_CONT(0.90) WITHIN GROUP( ORDER BY total_query_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct90_query_time_seconds
      , ROUND(( PERCENTILE_CONT(0.95) WITHIN GROUP( ORDER BY total_query_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct95_query_time_seconds
      ,SUM(cc_scaling) count_cc_scaling
      ,SUM(aborted) count_aborted
      ,SUM(1) count_queries
      , analysis_timestamp
FROM public.redshift_config_comparison_raw r
group by test_type,
       cluster_identifier,
       queue,
       username,
       analysis_timestamp
with no schema binding
;


drop view if exists public.redshift_config_comparison_results cascade;

create or replace view public.redshift_config_comparison_results as
with agg_data as (SELECT test_type,
       cluster_identifier,
       queue,
       username,
       total_query_time_seconds,
       max(total_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following) max_total_query_time_seconds,
       mean_query_time_seconds,
       max(mean_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_mean_query_time_seconds,
       median_query_time_seconds,
       max(median_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_median_query_time_seconds,
       max_query_time_seconds,
       max(max_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_max_query_time_seconds,
       pct75_query_time_seconds,
       max(pct75_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_pct75_query_time_seconds,
       pct90_query_time_seconds,
       max(pct90_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_pct90_query_time_seconds,
       pct95_query_time_seconds,
       max(pct95_query_time_seconds) over (partition by test_type,username rows between unbounded preceding and unbounded following ) max_pct95_query_time_seconds,
       count_cc_scaling,
       count_aborted,
       count_queries,
       analysis_timestamp
FROM public.redshift_config_comparison_aggregate)
select test_type,
 cluster_identifier,
       queue,
       username,
       total_query_time_seconds,
      round(((max_total_query_time_seconds-total_query_time_seconds)/case when total_query_time_seconds=0 then 1 else total_query_time_seconds end)*100)||'%'   improvement_total_query_time,
       mean_query_time_seconds,
	   round(((max_mean_query_time_seconds-mean_query_time_seconds)/case when mean_query_time_seconds=0 then 1 else mean_query_time_seconds end)*100)||'%'   improvement_mean_query_time,
       median_query_time_seconds,
       round(((max_median_query_time_seconds-median_query_time_seconds)/case when median_query_time_seconds=0 then 1 else median_query_time_seconds end)*100)||'%'   improvement_median_query_time,
       max_query_time_seconds,
       round(((max_max_query_time_seconds-max_query_time_seconds)/case when max_query_time_seconds=0 then 1 else max_query_time_seconds end)*100)||'%'   improvement_max_query_time,
       pct75_query_time_seconds,
        round(((max_pct75_query_time_seconds-pct75_query_time_seconds)/case when pct75_query_time_seconds=0 then 1 else pct75_query_time_seconds end)*100)||'%'   improvement_pct75_query_time,
       pct90_query_time_seconds,
        round(((max_pct90_query_time_seconds-pct90_query_time_seconds)/case when pct90_query_time_seconds=0 then 1 else pct90_query_time_seconds end)*100)||'%'   improvement_pct90_query_time,
       pct95_query_time_seconds,
      round(((max_pct95_query_time_seconds-pct95_query_time_seconds)/case when pct95_query_time_seconds=0 then 1 else pct95_query_time_seconds end)*100)||'%'   improvement_pct95_query_time,
      count_cc_scaling,
      count_aborted,
      count_queries,
      analysis_timestamp
 from agg_data
with no schema binding;



drop view if exists public.redshift_pricing cascade;

create or replace view public.redshift_pricing as
  select case
  when termtype='OnDemand' then priceperunit*365*24
  when purchaseoption='No Upfront' and leasecontractlength='1yr' then priceperunit*365*24
  when purchaseoption='All Upfront' and leasecontractlength='1yr' and unit='Quantity' then priceperunit
  when purchaseoption='No Upfront' and leasecontractlength='3yr' then priceperunit*365*24
  when purchaseoption='All Upfront' and leasecontractlength='3yr' and unit='Quantity' then priceperunit/3
  end::bigint per_compute_node_yearly_cost,
  unit,location, "instance type" instance_type, termtype,leasecontractlength,purchaseoption,unit
  FROM redshift_config_comparison.pricing  where  "product family"='Compute Instance'
  and nvl(purchaseoption,'OnDemand') in ('OnDemand','All Upfront','No Upfront')
  and priceperunit>0
with no schema binding;


drop view if exists public.redshift_config_comparison_pricing cascade;

create or replace view public.redshift_config_comparison_pricing as
    SELECT distinct
        c.node_type
        , c.number_of_nodes
        , NVL(p.termtype||'-'||p.leasecontractlength||'-'||p.purchaseoption,'On-Demand') options
        , p.per_compute_node_yearly_cost * c.number_of_nodes your_cluster_yearly_compute_cost
        , p.per_compute_node_yearly_cost
    FROM
        public.redshift_pricing p,
        redshift_config_comparison.cluster_config c
    WHERE  p.instance_type = c.node_type
           AND p.location = c.REGION
with no schema binding;



unload ($$
select * from public.redshift_detailed_query_stats where starttime > to_timestamp('{what_if_timestamp}','YYYY-MM-DD-HH24-MI-SS')
$$) to '{comparison_stats_s3_path}/{what_if_timestamp}/{cluster_identifier}/'
FORMAT AS PARQUET ALLOWOVERWRITE iam_role '{redshift_iam_role}';
