set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=app_district_will_sale_project;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;


--新开盘或者即将开盘楼盘名单：
drop table if exists tmp_etl.tmp_will_sale_project;
create table tmp_etl.tmp_will_sale_project as
select 
  t1.project_id,
  t1.name as project_name,
  t1.district_id,
  from_unixtime(t1.current_open_time,'yyyy-MM-dd') as current_open_date,
  t2.city_id 
  from  ods.trigger_project_house_type t1

  left join julive_dim.dim_district_info t2 on t1.district_id= t2.district_id

  where from_unixtime(t1.current_open_time,'yyyy-MM-dd')>=from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-30),'yyyy-MM-dd'),'yyyy-MM-dd')
  and from_unixtime(t1.current_open_time,'yyyy-MM-dd')<=from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),+30),'yyyy-MM-dd'),'yyyy-MM-dd')
  and t1.status!= 3
  and t1.is_show=1
  and t1.project_type in(1,2)
;

--各城市通用区域
drop table if exists tmp_etl.tmp_city_general_district;
create table tmp_etl.tmp_city_general_district as

select 
'99999999'     as district_id,
t1.city_name   as district_name,
t1.city_id     as city_id,
t1.city_name   as city_name,
'1'              as from_source
from 
(
select 
distinct city_id,city_name
from 
julive_dim.dim_district_info
where from_source =1) t1
;


--该区域存在的楼盘随机取一条
with district_real_will_sale_project as(
select 
tmp.* 

from (
select 
t1.project_id,
t1.project_name,
t1.district_id,
t1.city_id,
t1.current_open_date,
row_number() OVER (PARTITION BY t1.district_id ORDER BY rand() DESC) as rank_project

from tmp_etl.tmp_will_sale_project t1
)tmp
where tmp.rank_project=1

),

--随机取出城市某一个楼盘
city_real_will_sale_project as (
select 
tmp.* 

from(
select 
t1.project_id,
t1.project_name,
t1.district_id,
t1.city_id,
t1.current_open_date,
row_number() OVER (PARTITION BY t1.city_id ORDER BY rand() DESC) rank_project

from tmp_etl.tmp_will_sale_project t1
where district_id NOT IN ('20000032','20000034','20000033','20000031','20000030')
)tmp
where tmp.rank_project=1

)

insert overwrite table julive_app.app_district_will_sale_project
select 

t1.district_id,
if(t2.project_id is not null ,t1.district_name, t1.city_name)                   as district_name,
if(t2.project_id is not null ,t2.project_id,t3.project_id)                      as project_id,
if(t2.project_name is not null ,t2.project_name,t3.project_name)                as project_name,
t1.city_id,
t1.city_name,
if(t2.current_open_date is not null ,t2.current_open_date,t3.current_open_date) as current_open_date

from
(select 
district_id,
district_name,
city_id,
city_name,
from_source
from 
julive_dim.dim_district_info

union all 

select 
district_id,
district_name,
city_id,
city_name,
from_source

from tmp_etl.tmp_city_general_district
) t1 
left join district_real_will_sale_project t2 on t1.district_id = t2.district_id
left join city_real_will_sale_project t3 on t1.city_id= t3.city_id
where  t1.from_source=1
;







