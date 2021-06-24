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
  SELECT Trim(s.name)                                      queue,
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
         join stv_wlm_service_class_config s
           ON w.service_class = s.service_class
         left outer join queries
                      ON queries.query = q.query
         left outer join compiles ct
                      ON w.query = ct.query
  WHERE  q.userid > 1
         AND w.service_class > 5;


create or replace view public.redshift_config_comparison as
SELECT
    TRIM(query_label) sql_type
    , (total_query_time/1000000::float4) time_taken_sec
    , TRIM(cluster_identifier) cluster_identifier
    , TRIM(querytxt) query_text
    , starttime
    , endtime
    , queue
    , username
    , cc_scaling
    , aborted
    , queue_time
    , compile_time
    , exec_time
    , total_query_time
    , userid
    , query
    , xid
    , pid
    , service_class
    , tables_scanned
    , query_hash
    , whatif_timestamp
FROM
    redshift_config.query_stats q
WHERE    whatif_timestamp = (SELECT
                                 MAX(whatif_timestamp)
                             FROM
                                 redshift_config.query_stats)
         AND query_label IN ('run_concurrent_user_queries_and_load','run_ddl_and_copy_script','run_sequential_queries_and_load')
         and query_text not like 'padb_fetch_sample: %'
         and query_text not like 'CREATE TEMP TABLE volt_tt_%'
ORDER BY 1, 2
with no schema binding;


create or replace view public.redshift_config_comparison_aggregate as
  SELECT
        REPLACE(REPLACE(sql_type,'_user_queries_and_load',''),'run_','') test_type
        , cluster_identifier
        , COUNT(1) total_queries
        , cast(LEAST(SUM(time_taken_sec),DATEDIFF (microsecond, MIN(starttime), MAX(endtime)) / 1000000::FLOAT4) as decimal(10,2)) time_taken_seconds
        , cast(AVG(time_taken_sec) as decimal(10,2)) average_seconds_per_query
        , cast(MAX(time_taken_sec) as decimal(10,2)) maximum_seconds_per_query
        , cast(MIN(time_taken_sec) as decimal(10,2)) minimum_seconds_per_query
        , MIN(starttime) start_time
        , MAX(endtime) end_time
    FROM
        public.redshift_config_comparison
    GROUP BY
        sql_type, cluster_identifier
  order by 1,2
  with no schema binding;


unload ($$
select * from public.redshift_detailed_query_stats where starttime > to_timestamp('{what_if_timestamp}','YYYY-MM-DD-HH24-MI-SS')
$$) to 's3://{bucket_name}/query_stats/{what_if_timestamp}/{cluster_identifier}/'
FORMAT AS PARQUET ALLOWOVERWRITE iam_role '{redshift_iam_role}';
