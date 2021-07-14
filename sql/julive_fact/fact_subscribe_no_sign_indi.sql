------------------------------------------------------------------------------------
-- ETL -----------------------------------------------------------------------------
------------------------------------------------------------------------------------
set etl_date = concat_ws('-',substr(${hiveconf:etlDate},1,4),substr(${hiveconf:etlDate},5,2),substr(${hiveconf:etlDate},7,2));
---set etl_date = '2019-12-27'; -- 測試用 


set spark.app.name=fact_subscribe_no_sign_indi; 
set hive.execution.engine=spark;
set spark.default.parallelism=1400;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=4g;
set spark.executor.instances=14;
set spark.yarn.executor.memoryOverhead=4096;
set hive.prewarm.enabled=true;
set hive.prewarm.numcontainers=14;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000; 


insert overwrite table julive_fact.fact_subscribe_no_sign_indi partition(pdate)

select

deal_id,           
subscribe_id,
grass_sign_id,
clue_id,
                              
project_city_id,
project_city_name,
project_city_seq,
mgr_city_name,
mgr_city_seq,  

project_id,
project_name,
project_type,
                              
emp_id,
emp_name,
clue_emp_id,
clue_emp_name,

direct_leader_id,
direct_leader_name,
indirect_leader_id,
indirect_leader_name,
now_direct_leader_id,
now_direct_leader_name,
now_indirect_leader_id,
now_indirect_leader_name,

subscribe_status,
subscribe_type,
grass_sign_status,

is_have_risk,

receive_amt,
actual_amt,

subscribe_date,
actual_date,
grass_sign_date,
back_date,
plan_sign_date,

current_timestamp()  as etl_time,
${hiveconf:etl_date}         as pdate

from julive_fact.fact_subscribe_sign_payment_indi
where pdate=regexp_replace(${hiveconf:etl_date},'-','')
and sign_status is null 
and  (subscribe_status=1 or back_date>${hiveconf:etl_date})
and (subscribe_type=1 or subscribe_type=4)
;
