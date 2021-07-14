set hive.execution.engine=spark;
set spark.app.name=dim_preferred_open_info;
set mapred.job.name=dim_preferred_open_info;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
insert overwrite table julive_dim.dim_preferred_open_info

select 
tmp1.city_id,
tmp1.equipment_id,
tmp1.is_open,
lag(tmp1.create_date,1,'1970-01-01 08:00:00') over(partition by tmp1.city_id,tmp1.equipment_id order by tmp1.rn) as start_date,
tmp1.create_date as end_date,
current_timestamp()  as etl_time 
from
(select 
t1.city_id,
t1.equipment_id,
t1.is_open,
from_unixtime(t1.create_datetime) as create_date, 
row_number() over(partition by t1.city_id,t1.equipment_id order by t1.create_datetime) as rn
from 
(select 
tmp.city_id,
equipment_id,
tmp.is_open,
tmp.create_datetime
from
(select
city_id,
regexp_replace(equipment,'101,201','101201') as equipment,
is_open,
create_datetime
from
ods.yw_path_short_rule_history
where is_delete=1
) tmp
lateral VIEW
explode(split(tmp.equipment,",")) equipment AS equipment_id
union all
select 
tmp.city_id,
equipment_id,
tmp.is_open,
tmp.create_datetime
from
(select
city_id,
regexp_replace(equipment,'101,201','101201') as equipment,
is_open,
create_datetime
from
ods.yw_path_short_rule
where  is_delete=1
) tmp
lateral VIEW
explode(split(tmp.equipment,",")) equipment AS equipment_id 
) t1
)tmp1

union all 

select 
tmp2.* from 
(
select 
tmp1.city_id,
tmp1.equipment_id,
tmp1.is_open,
tmp1.create_date as start_date,
lead(tmp1.create_date,1,'9999-12-31 00:00:00') over(partition by tmp1.city_id,tmp1.equipment_id order by tmp1.rn) as end_date,
current_timestamp()  as etl_time 
from
(select 
t1.city_id,
t1.equipment_id,
t1.is_open,
from_unixtime(t1.create_datetime) as create_date, 
row_number() over(partition by t1.city_id,t1.equipment_id order by t1.create_datetime) as rn
from 
(select 
tmp.city_id,
equipment_id,
tmp.is_open,
tmp.create_datetime
from
(select
city_id,
regexp_replace(equipment,'101,201','101201') as equipment,
is_open,
create_datetime
from
ods.yw_path_short_rule_history
where is_delete=1
) tmp
lateral VIEW
explode(split(tmp.equipment,",")) equipment AS equipment_id
union all
select 
tmp.city_id,
equipment_id,
tmp.is_open,
tmp.create_datetime
from
(select
city_id,
regexp_replace(equipment,'101,201','101201') as equipment,
is_open,
create_datetime
from
ods.yw_path_short_rule
where  is_delete=1
) tmp
lateral VIEW
explode(split(tmp.equipment,",")) equipment AS equipment_id 
) t1
)tmp1
)tmp2
where tmp2.end_date='9999-12-31 00:00:00'
;


