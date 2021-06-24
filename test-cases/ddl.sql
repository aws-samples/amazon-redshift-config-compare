drop table if exists public.region;
drop table if exists public.nation;
drop table if exists public.supplier;
drop table if exists public.customer;
drop table if exists public.part;
drop table if exists public.partsupp;

create table region (
  r_regionkey int4 not null,
  r_name char(25) not null ,
  r_comment varchar(152) not null,
  Primary Key(R_REGIONKEY)
) distkey(r_regionkey) sortkey(r_regionkey);

create table nation (
  n_nationkey int4 not null,
  n_name char(25) not null ,
  n_regionkey int4 not null,
  n_comment varchar(152) not null,
  Primary Key(N_NATIONKEY)
) distkey(n_nationkey) sortkey(n_nationkey) ;

create table supplier (
  s_suppkey int4 not null,
  s_name char(25) not null,
  s_address varchar(40) not null,
  s_nationkey int4 not null,
  s_phone char(15) not null,
  s_acctbal numeric(12,2) not null,
  s_comment varchar(101) not null,
  Primary Key(S_SUPPKEY)
) distkey(s_suppkey) sortkey(s_suppkey)
;

create table customer (
  c_custkey int8 not null ,
  c_name varchar(25) not null,
  c_address varchar(40) not null,
  c_nationkey int4 not null,
  c_phone char(15) not null,
  c_acctbal numeric(12,2) not null,
  c_mktsegment char(10) not null,
  c_comment varchar(117) not null,
  Primary Key(C_CUSTKEY)
) distkey(c_custkey) sortkey(c_custkey);

create table part (
  p_partkey int8 not null ,
  p_name varchar(55) not null,
  p_mfgr char(25) not null,
  p_brand char(10) not null,
  p_type varchar(25) not null,
  p_size int4 not null,
  p_container char(10) not null,
  p_retailprice numeric(12,2) not null,
  p_comment varchar(23) not null,
  PRIMARY KEY (P_PARTKEY)
) distkey(p_partkey) sortkey(p_partkey);

create table partsupp (
  ps_partkey int8 not null,
  ps_suppkey int4 not null,
  ps_availqty int4 not null,
  ps_supplycost numeric(12,2) not null,
  ps_comment varchar(199) not null,
  Primary Key(PS_PARTKEY, PS_SUPPKEY)
) distkey(ps_partkey) sortkey(ps_partkey);

copy region from 's3://redshift-downloads/TPC-H/3TB/region/' iam_role '{redshift_iam_role}' gzip delimiter '|' region 'us-east-1';
copy nation from 's3://redshift-downloads/TPC-H/3TB/nation/' iam_role '{redshift_iam_role}' gzip delimiter '|' region 'us-east-1';
copy supplier from 's3://redshift-downloads/TPC-H/3TB/supplier/' iam_role '{redshift_iam_role}' gzip delimiter '|'  region 'us-east-1';
copy customer from 's3://redshift-downloads/TPC-H/3TB/customer/' iam_role '{redshift_iam_role}' gzip delimiter '|' region 'us-east-1';
copy part from 's3://redshift-downloads/TPC-H/3TB/part/' iam_role '{redshift_iam_role}' gzip delimiter '|'  region 'us-east-1';
copy partsupp from 's3://redshift-downloads/TPC-H/3TB/partsupp/' iam_role '{redshift_iam_role}' gzip delimiter '|' region 'us-east-1';
