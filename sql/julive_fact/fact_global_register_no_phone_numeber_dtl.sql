set etl_date = '${hiveconf:etlDate}'; -- 取昨天日期
set etl_tomorrow = '${hiveconf:etlTomorrow}'; -- 取前天日期

set hive.execution.engine=spark;
set spark.app.name=fact_global_register_no_phone_numeber_dtl;
set mapred.job.name=fact_global_register_no_phone_numeber_dtl;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table julive_fact.fact_global_register_no_phone_numeber_dtl partition (pdate)

select 
t1.global_id,
t1.user_id,         
coalesce(get_json_object(t1.properties,'$.register_user_id'),t1.julive_id) as julive_id,       
t1.comjia_unique_id,
t1.product_id,
t2.mobile   as user_mobile,   
t1.create_time,     
substr(t1.create_time,1,10) as first_register_date,
current_timestamp()         as etl_time,
t1.pdate 
from julive_fact.fact_event_dtl t1
left join  ods.cj_user t2 on t1.julive_id=t2.id 
where pdate =${hiveconf:etl_date}
and event in ('e_click_sign_up','app__v1__real__register2');


