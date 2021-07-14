set etl_date = '${hiveconf:etlDate}';

set hive.execution.engine=spark;
set spark.app.name=app_user_click_house_data;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table julive_app.app_user_click_house_data partition(pdate=${hiveconf:etl_date}) 

select
t1.global_id,
t1.comjia_unique_id,
t1.julive_id,
t1.user_id,
t1.product_id,
t1.house_id,
t1.create_time,
current_timestamp()                                            as etl_time

from
(
--用户点击户型详情页事件
--app
select
global_id,
comjia_unique_id,
julive_id,
user_id,
substr(product_id ,1,3) as product_id,
get_json_object(properties,'$.house_type_id') as house_id,
create_time

from julive_fact.fact_event_dtl 
where pdate = ${hiveconf:etl_date}
and event='e_page_view' 
and topage='p_house_type_details' 
and substr(product_id ,1,3) in ('101','201')

union all
--居理新房小程序 户型详情页 1天uv
select
global_id,
comjia_unique_id,
julive_id,
user_id,
product_id,

get_json_object(properties,'$.house_type_id') as house_id,
create_time
from julive_fact.fact_event_dtl 
where pdate = ${hiveconf:etl_date}
and event='e_page_view' 
and topage='p_house_type_details' 
and product_id in ('301','401')

union all

--居理新房PC，M站 户型详情页 1天uv 
select
global_id,
comjia_unique_id,
julive_id,
user_id,
product_id,

get_json_object(properties,'$.house_type_id') as house_id,
create_time
from julive_fact.fact_event_dtl 
where pdate = ${hiveconf:etl_date}
and event='e_page_view'
and topage='p_house_type_details' 
and product_id in ('1','2')

union all
--线上售楼处小程序,H5 户型详情页 
select
global_id,
comjia_unique_id,
julive_id,
user_id,
product_id,
get_json_object(properties,'$.house_type_id') as house_id,
create_time
from julive_fact.fact_event_dtl 
where pdate = ${hiveconf:etl_date}
and event='e_page_view' 
and topage='p_developer_house_type_details' 
and product_id in ('7','701')
) t1;
