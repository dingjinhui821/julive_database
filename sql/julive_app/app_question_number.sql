-- 增量ETL 
set hive.exectuion.engine=spark;
set spark.yarn.queue=etl;
set spark.app.name=app_question_number;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

drop table if exists tmp_dev_1.tmp_lastday_question_number;
create table tmp_dev_1.tmp_lastday_question_number as 

select 

question_id,
click_num 

from julive_app.app_question_number 
where pdate = regexp_replace(date_add(current_date(),-2),'-','') 
;


insert overwrite table julive_app.app_question_number partition(pdate) 

select 

regexp_replace(question_id,'\\[|\\]|"','') as question_id,
sum(click_num) as click_num,
regexp_replace(date_add(current_date(),-1),'-','') as pdate 

from (
select 

get_json_object(t1.properties,'$.question_id')  as question_id,
count(1) as click_num 
 
from julive_fact.fact_event_dtl t1 

where event = 'e_click_question_card' 
and fromPage = 'p_qa_home' 
and product_id in (2,101,201) 
and t1.pdate = regexp_replace(date_add(current_date(),-1),'-','') 
group by get_json_object(t1.properties,'$.question_id') 

union all 

select 

get_json_object(properties,'$.to_question_id') as question_id,
count(1) as click_num 

from julive_fact.fact_event_dtl t1 
where event = 'e_click_question_card' 
and fromPage = 'p_qa_details' 
and product_id in (2,101,201) 
and t1.pdate = regexp_replace(date_add(current_date(),-1),'-','') 
group by get_json_object(properties,'$.to_question_id') 

union all 

select 

question_id,
click_num 

from tmp_dev_1.tmp_lastday_question_number
) t  
where question_id is not null 
group by regexp_replace(question_id,'\\[|\\]|"','')
;

