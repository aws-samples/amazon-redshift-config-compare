--first_query

SELECT
    s_acctbal
    , s_name
    , n_name
    , p_partkey
    , p_mfgr
    , s_address
    , s_phone
    , s_comment
FROM
    part,
    supplier,
    partsupp,
    nation,
    REGION
WHERE    p_partkey = ps_partkey
         AND s_suppkey = ps_suppkey
         AND p_size = 34
         AND p_type LIKE '%COPPER'
         AND s_nationkey = n_nationkey
         AND n_regionkey = r_regionkey
         AND r_name = 'MIDDLE EAST'
         AND ps_supplycost = (SELECT
                                  MIN(ps_supplycost)
                              FROM
                                  partsupp,
                                  supplier,
                                  nation,
                                  REGION
                              WHERE  p_partkey = ps_partkey
                                     AND s_suppkey = ps_suppkey
                                     AND s_nationkey = n_nationkey
                                     AND n_regionkey = r_regionkey
                                     AND r_name = 'MIDDLE EAST')
ORDER BY
    s_acctbal DESC
    , n_name
    , s_name
    , p_partkey ;

--second_query

SELECT
    ps_partkey
    , SUM(ps_supplycost * ps_availqty) AS value
FROM
    partsupp,
    supplier,
    nation
WHERE    ps_suppkey = s_suppkey
         AND s_nationkey = n_nationkey
         AND n_name = 'SAUDI ARABIA'
GROUP BY
    ps_partkey
HAVING
     SUM(ps_supplycost * ps_availqty) > (SELECT
                                             SUM(ps_supplycost * ps_availqty) * 0.0000000333
                                         FROM
                                             partsupp,
                                             supplier,
                                             nation
                                         WHERE  ps_suppkey = s_suppkey
                                                AND s_nationkey = n_nationkey
                                                AND n_name = 'SAUDI ARABIA')
ORDER BY
    value DESC ;

--third_query

SELECT
    p_brand
    , p_type
    , p_size
    , COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM
    partsupp,
    part
WHERE    p_partkey = ps_partkey
         AND p_brand <> 'Brand#23'
         AND p_type NOT LIKE 'MEDIUM ANODIZED%'
         AND p_size IN (1, 32, 33, 46, 7, 42, 21, 40)
         AND ps_suppkey NOT IN (SELECT
                                    s_suppkey
                                FROM
                                    supplier
                                WHERE  s_comment LIKE '%Customer%Complaints%')
GROUP BY
    p_brand
    , p_type
    , p_size
ORDER BY
    supplier_cnt DESC
    , p_brand
    , p_type
    , p_size ;


--fourth_query

SELECT r_name,count(1) number_of_supplies
      FROM
          part,
          partsupp,
          supplier,
          nation,
          REGION
      WHERE  p_partkey = ps_partkey
              AND s_suppkey = ps_suppkey
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              group by 1
              order by 1;


--fifth_query

SELECT
    n_name
    , COUNT(1) total_count
FROM
    supplier,
    nation
WHERE    s_suppkey IN (SELECT
                           ps_suppkey
                       FROM
                           partsupp
                       WHERE  ps_partkey IN (SELECT
                                                 p_partkey
                                             FROM
                                                 part
                                             WHERE  p_name LIKE 'olive%')
                              AND ps_availqty > 1)
         AND s_nationkey = n_nationkey
GROUP BY
    1
ORDER BY
    1;
