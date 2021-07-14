set hive.execution.engine=spark;
set spark.app.name=dim_district_info;
set spark.yarn.queue=etl;
--省份信息
with province_information_tmp as(

select 
* 
from 
ods.cj_district
where parent_id = 0

),
--市级信息
city_information_tmp as(

select 
t1.* 
from 
ods.cj_district t1 
join province_information_tmp t2 on t1.parent_id = t2.id


),

--区级信息
district_infomation_tmp as(

select 
t1.* 
from 
ods.cj_district t1 
join city_information_tmp t2 on t1.parent_id = t2.id

)

insert overwrite table julive_dim.dim_district_info

select 
t1.id           as district_id,
t1.name_cn      as district_name,
t1.name_pinyin  as district_name_cn,
t1.parent_id    as city_id,
t2.name_cn      as city_name,
t2.name_pinyin  as city_name_cn,
t2.parent_id    as province_id,
t3.name_cn      as province_name,
t3.name_pinyin  as province_name_cn,

t1.coordinate   as coordinate,

case 
when t4.city_id is null                        then 1
when t4.city_id is not null and from_source =2 then 2
when t4.city_id is not null and from_source =3 then 3
else 999  end    as from_source


from 
district_infomation_tmp t1
left join city_information_tmp t2 on t1.parent_id = t2.id 
left join province_information_tmp t3 on t2.parent_id = t3.id

left join julive_dim.dim_wlmq_city t4 on t2.id = t4.city_id

;
