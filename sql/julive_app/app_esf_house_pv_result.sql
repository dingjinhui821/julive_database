set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=app_esf_house_pv_result;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table julive_app.app_esf_house_pv_result 

select 
t2.select_city                               as city_id,
concat_ws(',',collect_list(t2.esf_house_id)) as hot_search_esf
from
(
select 
t1.select_city,
t1.esf_house_id,
t1.pv,
row_number() over(partition by t1.select_city order by t1.pv desc ) as pv_ranking
from(
select 
tmp1.select_city,
tmp1.esf_house_id,
tmp1.pv
from
(select
cast(select_city as int) as select_city,
esf_house_id,
count(1) as pv
from

julive_app.app_esf_house_pv

where pdate <=${hiveconf:etl_date}
  and pdate >=from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-14),'yyyy-MM-dd'),'yyyyMMdd')

group by select_city,
esf_house_id) tmp1

left join 
(select id,city_id
from ods.xpt_house
where city_id in(98,99)) tmp2 on tmp1.esf_house_id=tmp2.id
join ods.xpt_house tmp3 on tmp1.esf_house_id = tmp3.id 

where tmp2.id is null
 and tmp3.is_hidden =2
)t1
)t2

where t2.pv_ranking<=50

group by t2.select_city;



