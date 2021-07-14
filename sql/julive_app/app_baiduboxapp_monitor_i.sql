-- 增量ETL ------------------------------------
-- 增量ETL ------------------------------------
-- 增量ETL ------------------------------------
set etl_date = '${hiveconf:etldate}'; 
set etl_yestoday = '${hiveconf:etlyestoday}'; 

set hive.execution.engine=spark;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.app.name=app_baiduboxapp_monitor_i;
set spark.yarn.queue=etl;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;


drop table if exists tmp_dev_1.tmp_baiduboxapp_monitor_yestoday;
create table tmp_dev_1.tmp_baiduboxapp_monitor_yestoday as 

select 

user_agent,
baiduboxapp 

from julive_app.app_baiduboxapp_monitor_i 
where pdate = ${hiveconf:etl_yestoday} 
;


insert overwrite table julive_app.app_baiduboxapp_monitor_i partition(pdate) 

select 

user_agent,
concat(
case 
when instr(lower(user_agent),'iphone') > 0 then "ios_" 
when instr(lower(user_agent),'android') > 0 then "android_" 
end,
regexp_extract(user_agent,"baiduboxapp\\/[\\d|\\.]+",0)
) as baiduboxapp,
${hiveconf:etl_date} as pdate 

from (
SELECT 

user_agent

FROM julive_fact.fact_event_dtl 
WHERE pdate = ${hiveconf:etl_date} 
  AND event = 'e_page_view' 
  AND product_id = 2
  AND user_agent LIKE '%baiduboxapp%'
GROUP BY user_agent

union all 

select 

user_agent

from tmp_dev_1.tmp_baiduboxapp_monitor_yestoday 
) t 
group by user_agent
;

