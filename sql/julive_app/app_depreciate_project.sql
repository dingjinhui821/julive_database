set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=app_depreciate_project;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table julive_app.app_depreciate_project
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
  project_id,
  final_price,
  cut_price,
  final_create_date,
  last_but_one_create_date,
  row_number() over(partition by project_id order by cut_price desc) as cut_pricern
  from 
  julive_app.app_house_price_change
  where price_status =1
  and  last_but_one_price <10000000
  and  cut_price<600000
  and final_price >0
 )t1 --降价楼盘
join ods.cj_project t2 on t1.project_id = t2.project_id  
where t1.cut_pricern =1
and t2.status!= 3
and t2.is_show=1
and t2.project_type in(1,2)
and t1.last_but_one_create_date>from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-90),'yyyy-MM-dd'),'yyyy-MM-dd')
) tmp
where tmp.rn<=5;
