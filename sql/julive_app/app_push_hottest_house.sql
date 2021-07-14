set hive.execution.engine=spark;
set spark.app.name=app_push_hottest_house;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table julive_app.app_push_hottest_house
select 
tmp1.city_id,
tmp1.project_id,
tmp1.house_id,
tmp1.room_type,
tmp1.good_desc,
tmp1.house_tag,
tmp1.master_bed_room,
tmp1.toilet,
tmp1.living_room,
tmp1.status,
tmp1.house_uv,
tmp1.hot_sort
from
(

select 
tmp.city_id,
tmp.project_id,
tmp.house_id,
t3.room_type,
t3.good_desc,
t3.house_tag,
t3.master_bed_room,
t3.toilet,
t3.living_room,
t3.status,
tmp.num  as house_uv,
tmp.rn   as hot_sort
from
(
select 
t2.city_id,
t1.project_id,
t1.house_id,
t1.num,
row_number() over(partition by t2.city_id order by t1.num desc) as rn
from 
(select 
project_id,
house_id,
sum(uv_one_day) as num 
from julive_app.app_house_uv 
group by project_id,house_id
) t1 
join
(
select
city_id,project_id
from
julive_dim.dim_project_info group by city_id,project_id ) t2 
on t1.project_id = t2.project_id
)tmp
 join ods.cj_house_type_batch t3 on tmp.house_id = t3.house_type_id
where t3.status !=3
and tmp.num>10
)tmp1
where tmp1.hot_sort<=5
;

