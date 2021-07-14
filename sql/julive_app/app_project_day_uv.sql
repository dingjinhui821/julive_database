set mapred.job.name=app_project_day_uv;
set mapreduce.job.queuename=root.etl;
drop table if exists julive_app.app_project_day_uv;
create table julive_app.app_project_day_uv as 

select 

coalesce(tmp2.pdate,tmp1.pdate) as pdate,
coalesce(tmp2.time_p,tmp1.time_p) as time_p,
coalesce(tmp2.city_id,tmp1.city_id) as city_id,
coalesce(tmp2.project_id,tmp1.project_id) as project_id,
coalesce(tmp2.uv,tmp1.uv) as uv 

from ( 
select 

t1.date_id  as pdate,
t1.date_str as time_p,
t2.city_id,
t2.project_id,
0 as uv 

from (
select date_id,date_str 
from julive_dim.dim_date 
where date_str <= current_date() 
and date_str >= '2019-12-01' 
) t1 join (
select 

city_id,
project_id

from julive_app.tmp_cjw_project_uv t 
group by 
city_id,
project_id
) t2 
) tmp1 
left join julive_app.tmp_cjw_project_uv tmp2 on tmp1.pdate = tmp2.pdate and tmp1.project_id = tmp2.project_id 
;

