set mapred.job.name=app_project_day_clue_num;
set mapreduce.job.queuename=root.etl;
drop table if exists julive_app.app_project_day_clue_num;
create table julive_app.app_project_day_clue_num as 

select 

coalesce(tmp2.order_time,tmp1.order_time) as order_time,
coalesce(tmp2.city_id,tmp1.city_id) as city_id,
coalesce(tmp2.project_id,tmp1.project_id) as project_id,
coalesce(tmp2.sum_order,tmp1.sum_order) as sum_order 

from ( 
select 

t1.date_str as order_time,
t2.city_id,
t2.project_id,
0 as sum_order 

from (
select date_id,date_str 
from julive_dim.dim_date 
where date_str <= current_date() 
and date_str >= '2019-12-01' 
) t1 join (
select 

city_id,
project_id

from julive_app.tmp_cjw_project_order t 
group by 
city_id,
project_id
) t2 
) tmp1 
left join julive_app.tmp_cjw_project_order tmp2 on tmp1.order_time = tmp2.order_time and tmp1.project_id = tmp2.project_id 
;

