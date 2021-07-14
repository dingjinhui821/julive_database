set hive.execution.engine=spark;
set spark.app.name=app_short_url_info;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048; 
set hive.exec.dynamic.partition.mode=nonstrict;

drop table if exists julive_app.app_short_url_info;
create table julive_app.app_short_url_info as

select 
media_id,
account_id,
plan_id,
unit_id,

pc_short_url,
pc_channel_id,
m_short_url,
m_channel_id,
count(distinct plan_id)  as plan_num,
count(distinct unit_id)  as unit_num,
count(distinct keyword_id) as keyword_num

from julive_dim.dim_keyword_info
 where 
 pdate = regexp_replace(date_add(current_date(),-1),"-","")
 and pause = 0
 group by account_id,
media_id,
pc_short_url,
pc_channel_id,
m_short_url,
m_channel_id,
plan_id,
unit_id;

