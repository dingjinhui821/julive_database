set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=app_esf_house_strict_selection;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;


--二手房城市区域清单
drop table if exists tmp_etl.tmp_esf_city_real_district;
create table tmp_etl.tmp_esf_city_real_district as
select
distinct
t2.district_id,
t2.district_name,
t2.city_id,
t2.city_name
from ods.xpt_house t1
join julive_dim.dim_district_info t2 on t1.city_id = t2.city_id
where t1.is_select =1
and t1.is_hidden=2
;

--近三十天点击二手房详情页的人
drop table if exists julive_app.app_esf_click_crowd;
create table julive_app.app_esf_click_crowd as
select  
t1.comjia_unique_id,
t1.comjia_platform_id as product_id,
t1.select_city  as city_id,
t1.esf_house_id as house_id,
t2.district_id
from(
select  
comjia_unique_id,
comjia_platform_id,
select_city,
esf_house_id,
row_number() over(partition by comjia_unique_id order by create_time desc) as rn
from julive_app.app_esf_house_pv 
where pdate >=from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-30),'yyyy-MM-dd'),'yyyyMMdd')
and comjia_platform_id in(101,201)
) t1
left join ods.xpt_house t2 on t1.esf_house_id =t2.id 
where t1.rn=1
and t1.select_city in(select distinct city_id from tmp_etl.tmp_esf_city_real_district)

;

--二手房城市通用区域
drop table if exists tmp_etl.tmp_esf_city_general_district;
create table tmp_etl.tmp_esf_city_general_district as
select 
'99999999'     as district_id,
t2.city_name   as district_name,
t1.city_id     as city_id,
t2.city_name   as city_name
from 
(
select 
distinct city_id
from 
ods.xpt_house
where is_select =1
and is_hidden=2
)t1
left join julive_dim.dim_city t2 on t1.city_id =t2.city_id
;


--该区域严选楼盘随便取1条
with district_real_esf_house as(
select 
tmp.* 

from (
select 
t1.id   as house_id,
t1.village_name,
t1.district_id,
t1.city_id,
t1.total_price,
row_number() OVER (PARTITION BY t1.district_id ORDER BY rand() DESC) as rank_project
from ods.xpt_house t1
where t1.is_select=1
and t1.is_hidden=2
and t1.total_price <=10000000
)tmp
where tmp.rank_project=1

),

--随机取出1个城市居理严选房源id
city_real_esf_house as (
select 
tmp.* 
from(
select 
t1.id  as house_id,
t1.village_name,
t1.district_id,
t1.city_id,
t1.total_price,
row_number() OVER (PARTITION BY t1.city_id ORDER BY rand() DESC) as rank_project
from ods.xpt_house t1
where t1.is_select=1
and t1.is_hidden=2
and t1.total_price <=10000000
and t1.district_id NOT IN ('20000032','20000034','20000033','20000031','20000030')
)tmp
where tmp.rank_project=1

)

insert overwrite table julive_app.app_esf_house_strict_selection
select 

t1.district_id,
if(t2.house_id is not null ,t1.district_name,t1.city_name)                      as district_name,
if(t2.house_id is not null ,t2.house_id,t3.house_id)                            as house_id,
if(t2.village_name is not null ,t2.village_name,t3.village_name)                as village_name,
if(CEILING(t2.total_price/10000) is not null,CEILING(t2.total_price/10000),CEILING(t3.total_price/10000))                    as total_price,
t1.city_id,
t1.city_name


from
(select 
district_id,
district_name,
city_id,
city_name
from 
tmp_etl.tmp_esf_city_real_district

union all 

select 
district_id,
district_name,
city_id,
city_name

from tmp_etl.tmp_esf_city_general_district
) t1 
left join district_real_esf_house t2 on t1.district_id = t2.district_id
left join city_real_esf_house t3 on t1.city_id= t3.city_id
;






