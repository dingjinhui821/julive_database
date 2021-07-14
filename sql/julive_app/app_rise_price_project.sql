set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=app_rise_price_project;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;

--近一年热销户型
drop table if exists tmp_etl.tmp_one_year_house_uv;
create table tmp_etl.tmp_one_year_house_uv as
select 
t1.house_id,
t1.uv,
t2.project_id,
t3.name        as project_name,
t3.city_id,
row_number() over(partition by t3.city_id order by t1.uv desc) as uv_ranking
from
(
  select 
  house_id,
  count(distinct global_id) as uv
  from
  julive_app.app_user_click_house_data
  where pdate>=from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-365),'yyyy-MM-dd'),'yyyyMMdd')
  and pdate<=${hiveconf:etl_date}
    group by house_id
) t1
join julive_dim.dim_house_info t2 on t1.house_id = t2.house_id
left join ods.cj_project t3 on t2.project_id = t3.project_id
where 
t2.status !=3

;

--近一年涨价楼盘


insert overwrite table julive_app.app_rise_price_project
select
tmp.project_id,
tmp.project_name,
tmp.project_city,
CEILING(tmp.final_price/10000) as final_price,
CEILING(tmp.cut_price/10000)   as cut_price,
tmp.district_id,
tmp.district_name,
tmp.final_create_date
from
(
select
t1.project_id,
t2.name        as project_name,
t2.city_id     as project_city,
t1.final_price,
t1.cut_price,
t1.final_create_date,
t2.district_id,
t2.district_name,
row_number() over(partition by t2.city_id order by t1.cut_price desc ) as rn
from(
 select
  t1.project_id,
  t1.final_price,
  t1.cut_price,
  t1.final_create_date,
  t1.last_but_one_create_date,
  row_number() over(partition by t1.project_id order by t1.cut_price desc) as cut_pricern
  from
  julive_app.app_house_price_change t1
  join tmp_etl.tmp_one_year_house_uv t2 on t1.house_id =t2.house_id
  where t1.price_status =2
   and  t1.last_but_one_price <10000000
   and  t1.cut_price<600000
   and  t1.final_price >0
   and  t2.uv_ranking<1000
 )t1 --涨价楼盘
join ods.cj_project t2 on t1.project_id = t2.project_id
where t1.cut_pricern =1
and t2.status!= 3
and t2.is_show=1
and t2.project_type in(1,2)
and t1.last_but_one_create_date>from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-90),'yyyy-MM-dd'),'yyyy-MM-dd')
) tmp
where tmp.rn<=5;

