set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=app_esf_house_pv;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table julive_app.app_esf_house_pv partition(pdate=${hiveconf:etl_date})
select 
t1.global_id,
t1.comjia_unique_id,
t1.user_id,
t1.julive_id,
t1.product_id,
t2.city_id as select_city,
cast(t1.esf_house_id as int) as esf_house_id,
t1.manufacturer,
t1.app_version,
t1.os,
t1.os_version,
t1.comjia_platform_id,
t1.create_time,

current_timestamp()                                            as etl_time
from
(
select 
global_id,
comjia_unique_id,
user_id,
julive_id,
product_id,
select_city,
get_json_object(properties,'$.esf_house_id') as esf_house_id,
get_json_object(properties,'$.manufacturer') as manufacturer,
app_version,
get_json_object(properties,'$.os') as os,
get_json_object(properties,'$.os_version') as os_version,
substr(get_json_object(properties,'$.comjia_platform_id'),1,3) as comjia_platform_id,
create_time

from julive_fact.fact_event_esf_dtl
where
  pdate = ${hiveconf:etl_date} 
  and event='e_page_view'
  and topage='p_esf_house_details'
  and product_id='20'
union all 
select 
global_id,
comjia_unique_id,
user_id,
julive_id,
product_id,
select_city,
get_json_object(properties,'$.esf_house_id') as esf_house_id,
get_json_object(properties,'$.manufacturer') as manufacturer,
app_version,
get_json_object(properties,'$.os') as os,
get_json_object(properties,'$.os_version') as os_version,
substr(get_json_object(properties,'$.comjia_platform_id'),1,3) as comjia_platform_id,
create_time
from julive_fact.fact_event_dtl
where
  pdate =${hiveconf:etl_date}
  and event='e_page_view'
  and topage='p_esf_house_details'
  and product_id in ('101','201') 
)t1
join ods.xpt_house t2 on t1.esf_house_id =t2.id 
