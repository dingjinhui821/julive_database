set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=app_esf_house_minprice;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;



--该区域低总价严选楼盘随便取1条
with district_real_esf_house_minprice as(
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
from 
(select id,
village_name,
district_id,
city_id,
total_price,
row_number() OVER (PARTITION BY district_id ORDER BY total_price) as rn_house
from ods.xpt_house 
where is_select=1
and is_hidden=2) t1
where t1.rn_house<=5
)tmp
where tmp.rank_project=1

),

--随机取出1个城市居理严选房源id
city_real_esf_house_minprice as (
select 
tmp.* 
from(
select 
t1.id   as house_id,
t1.village_name,
t1.district_id,
t1.city_id,
t1.total_price,
row_number() OVER (PARTITION BY t1.city_id ORDER BY rand() DESC) as rank_project
from 
(select id,
village_name,
district_id,
city_id,
total_price,
row_number() OVER (PARTITION BY city_id ORDER BY total_price) as rn_house
from ods.xpt_house 
where is_select=1
and is_hidden=2
and district_id NOT IN ('20000032','20000034','20000033','20000031','20000030')) t1
where t1.rn_house<=5
)tmp
where tmp.rank_project=1

)

insert overwrite table julive_app.app_esf_house_minprice
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
left join district_real_esf_house_minprice t2 on t1.district_id = t2.district_id
left join city_real_esf_house_minprice t3 on t1.city_id= t3.city_id
;

