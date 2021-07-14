-- -----------------------------------------------------------------------------------
-- -----------------------------------------------------------------------------------
-- -----------------------------------------------------------------------------------
-- 增量 etl script 
set etl_date = '${hiveconf:etldate}'; 
set etl_yestoday = '${hiveconf:etlyestoday}'; 

-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); 
-- set etl_yestoday = regexp_replace(date_add(current_date(),-2),'-',''); 
set hive.execution.engine=spark;
set spark.app.name=app_event_video_click_number;
set spark.yarn.queue=etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;



drop table if exists tmp_dev_1.tmp_event_video_click_number;
create table tmp_dev_1.tmp_event_video_click_number as 

select video_id,total 
from julive_app.app_event_video_click_number 
where pdate = ${hiveconf:etl_yestoday}
;


insert overwrite table julive_app.app_event_video_click_number partition(pdate) 

select 

video_id,
sum(total) as total,
${hiveconf:etl_date} as pdate 

from (
select 

get_json_object(t.properties,'$.video_id') as video_id,
count(1) as total

from julive_fact.fact_event_dtl t -- 对应神策events 
where event = 'e_video_play' 
and substr(product_id,1,3) in ('101','201') 
and (get_json_object(properties,'$.is_wifi') != '1' or get_json_object(properties,'$.is_wifi') is null) -- 排除自动播放 
and pplatform = '103' -- 101 201 
and pdate = ${hiveconf:etl_date} -- 取昨天日期 

group by get_json_object(t.properties,'$.video_id')

union all 

select 

video_id,
total 

from tmp_dev_1.tmp_event_video_click_number 
) tmp 
group by video_id 
;


