
-- job2 : dim_department_info 
-- -----------------------------------------------------------------------

set etl_date = concat_ws('-',substr(${hiveconf:etlDate},1,4),substr(${hiveconf:etlDate},5,2),substr(${hiveconf:etlDate},7,2)); 
set etl_yestoday = date_add(${hiveconf:etl_date},-1);
set end_date = '9999-12-31';

set hive.execution.engine=spark;
set spark.app.name=dim_department_info;
set mapred.job.name=dim_department_info;
set spark.yarn.queue=etl;
set mapreduce.job.queuename=root.etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

-- 缓慢变化维计算 
from (
select * 
from tmp_dev_1.tmp_dept_history_data 
where pdate = ${hiveconf:etl_yestoday}
) h full outer join (
select * 
from tmp_dev_1.tmp_dept_current_data 
where pdate = ${hiveconf:etl_date} 
) c on h.dept_id = c.dept_id 

-- 闭链 
insert overwrite table julive_dim.dim_department_info partition(end_date) 

select 

h.skey,

h.dept_id,
h.dept_name,
h.dept_level,
h.team_leader_id,
h.team_leader_name,
h.city_id,
h.city_name,
h.cate_id,
h.cate_name,
h.dept_type_id,
h.dept_type_name,
h.dept_attr,
h.dept_level_path,
h.dept_level_leader,

h.dept_id_first,
h.dept_name_first,
h.dept_leader_id_first,
h.dept_leader_name_first,
h.dept_id_second,
h.dept_name_second,
h.dept_leader_id_second,
h.dept_leader_name_second,
h.dept_id_third,
h.dept_name_third,
h.dept_leader_id_third,
h.dept_leader_name_third,
h.dept_id_fourth,
h.dept_name_fourth,
h.dept_leader_id_fourth,
h.dept_leader_name_fourth,
h.dept_id_fifth,
h.dept_name_fifth,
h.dept_leader_id_fifth,
h.dept_leader_name_fifth,
h.dept_id_sixth,
h.dept_name_sixth,
h.dept_leader_id_sixth,
h.dept_leader_name_sixth,
h.dept_id_seventh,
h.dept_name_seventh,
h.dept_leader_id_seventh,
h.dept_leader_name_seventh,
h.dept_id_eighth,
h.dept_name_eighth,
h.dept_leader_id_eighth,
h.dept_leader_name_eighth,

h.create_date,
1 as is_archived,

h.version,
0 as status,
h.start_date,
current_timestamp() as etl_time,
${hiveconf:etl_date} as end_date 

where h.dept_id is not null -- 能够join上的数据 
and c.dept_id is not null 

and ( -- 并且有变化的数据 

coalesce(h.dept_name,'-999') != coalesce(c.dept_name,'-999') or 
coalesce(h.team_leader_id,'-999') != coalesce(c.team_leader_id,'-999') or 
coalesce(h.team_leader_name,'-999') != coalesce(c.team_leader_name,'-999') or 
coalesce(h.city_id,'-999') != coalesce(c.city_id,'-999') or 
coalesce(h.cate_id,'-999') != coalesce(c.cate_id,'-999') or 
coalesce(h.dept_type_id,'-999') != coalesce(c.dept_type_id,'-999') or 
coalesce(h.dept_level_path,'-999') != coalesce(c.dept_level_path,'-999') or 

coalesce(h.dept_id_first,'-999') != coalesce(c.dept_id_first,'-999') or 
coalesce(h.dept_name_first,'-999') != coalesce(c.dept_name_first,'-999') or 
coalesce(h.dept_leader_id_first,'-999') != coalesce(c.dept_leader_id_first,'-999') or 
coalesce(h.dept_leader_name_first,'-999') != coalesce(c.dept_leader_name_first,'-999') or 

coalesce(h.dept_id_second,'-999') != coalesce(c.dept_id_second,'-999') or 
coalesce(h.dept_name_second,'-999') != coalesce(c.dept_name_second,'-999') or 
coalesce(h.dept_leader_id_second,'-999') != coalesce(c.dept_leader_id_second,'-999') or 
coalesce(h.dept_leader_name_second,'-999') != coalesce(c.dept_leader_name_second,'-999') or 

coalesce(h.dept_id_third,'-999') != coalesce(c.dept_id_third,'-999') or  
coalesce(h.dept_name_third,'-999') != coalesce(c.dept_name_third,'-999') or  
coalesce(h.dept_leader_id_third,'-999') != coalesce(c.dept_leader_id_third,'-999') or  
coalesce(h.dept_leader_name_third,'-999') != coalesce(c.dept_leader_name_third,'-999') or  

coalesce(h.dept_id_fourth,'-999') != coalesce(c.dept_id_fourth,'-999') or 
coalesce(h.dept_name_fourth,'-999') != coalesce(c.dept_name_fourth,'-999') or  
coalesce(h.dept_leader_id_fourth,'-999') != coalesce(c.dept_leader_id_fourth,'-999') or 
coalesce(h.dept_leader_name_fourth,'-999') != coalesce(c.dept_leader_name_fourth,'-999') or  

coalesce(h.dept_id_fifth,'-999') != coalesce(c.dept_id_fifth,'-999') or 
coalesce(h.dept_name_fifth,'-999') != coalesce(c.dept_name_fifth,'-999') or 
coalesce(h.dept_leader_id_fifth,'-999') != coalesce(c.dept_leader_id_fifth,'-999') or 
coalesce(h.dept_leader_name_fifth,'-999') != coalesce(c.dept_leader_name_fifth,'-999') or 

coalesce(h.dept_id_sixth,'-999') != coalesce(c.dept_id_sixth,'-999') or 
coalesce(h.dept_name_sixth,'-999') != coalesce(c.dept_name_sixth,'-999') or 
coalesce(h.dept_leader_id_sixth,'-999') != coalesce(c.dept_leader_id_sixth,'-999') or 
coalesce(h.dept_leader_name_sixth,'-999') != coalesce(c.dept_leader_name_sixth,'-999') or 

coalesce(h.dept_id_seventh,'-999') != coalesce(c.dept_id_seventh,'-999') or 
coalesce(h.dept_name_seventh,'-999') != coalesce(c.dept_name_seventh,'-999') or 
coalesce(h.dept_leader_id_seventh,'-999') != coalesce(c.dept_leader_id_seventh,'-999') or 
coalesce(h.dept_leader_name_seventh,'-999') != coalesce(c.dept_leader_name_seventh,'-999') or 

coalesce(h.dept_id_eighth,'-999') != coalesce(c.dept_id_eighth,'-999') or 
coalesce(h.dept_name_eighth,'-999') != coalesce(c.dept_name_eighth,'-999') or 
coalesce(h.dept_leader_id_eighth,'-999') != coalesce(c.dept_leader_id_eighth,'-999') or 
coalesce(h.dept_leader_name_eighth,'-999') != coalesce(c.dept_leader_name_eighth,'-999') 

)

-- 开链 
insert overwrite table julive_dim.dim_department_info partition(end_date) 

select 

-- 计算主键：skey
case 
when h.dept_id is not null and c.dept_id is not null and ( 
coalesce(h.dept_name,'-999') != coalesce(c.dept_name,'-999') or 
coalesce(h.team_leader_id,'-999') != coalesce(c.team_leader_id,'-999') or 
coalesce(h.team_leader_name,'-999') != coalesce(c.team_leader_name,'-999') or 
coalesce(h.city_id,'-999') != coalesce(c.city_id,'-999') or 
coalesce(h.cate_id,'-999') != coalesce(c.cate_id,'-999') or 
coalesce(h.dept_type_id,'-999') != coalesce(c.dept_type_id,'-999') or 
coalesce(h.dept_level_path,'-999') != coalesce(c.dept_level_path,'-999') or 

coalesce(h.dept_id_first,'-999') != coalesce(c.dept_id_first,'-999') or 
coalesce(h.dept_name_first,'-999') != coalesce(c.dept_name_first,'-999') or 
coalesce(h.dept_leader_id_first,'-999') != coalesce(c.dept_leader_id_first,'-999') or 
coalesce(h.dept_leader_name_first,'-999') != coalesce(c.dept_leader_name_first,'-999') or 

coalesce(h.dept_id_second,'-999') != coalesce(c.dept_id_second,'-999') or 
coalesce(h.dept_name_second,'-999') != coalesce(c.dept_name_second,'-999') or 
coalesce(h.dept_leader_id_second,'-999') != coalesce(c.dept_leader_id_second,'-999') or 
coalesce(h.dept_leader_name_second,'-999') != coalesce(c.dept_leader_name_second,'-999') or 

coalesce(h.dept_id_third,'-999') != coalesce(c.dept_id_third,'-999') or  
coalesce(h.dept_name_third,'-999') != coalesce(c.dept_name_third,'-999') or  
coalesce(h.dept_leader_id_third,'-999') != coalesce(c.dept_leader_id_third,'-999') or  
coalesce(h.dept_leader_name_third,'-999') != coalesce(c.dept_leader_name_third,'-999') or  

coalesce(h.dept_id_fourth,'-999') != coalesce(c.dept_id_fourth,'-999') or 
coalesce(h.dept_name_fourth,'-999') != coalesce(c.dept_name_fourth,'-999') or  
coalesce(h.dept_leader_id_fourth,'-999') != coalesce(c.dept_leader_id_fourth,'-999') or 
coalesce(h.dept_leader_name_fourth,'-999') != coalesce(c.dept_leader_name_fourth,'-999') or  

coalesce(h.dept_id_fifth,'-999') != coalesce(c.dept_id_fifth,'-999') or 
coalesce(h.dept_name_fifth,'-999') != coalesce(c.dept_name_fifth,'-999') or 
coalesce(h.dept_leader_id_fifth,'-999') != coalesce(c.dept_leader_id_fifth,'-999') or 
coalesce(h.dept_leader_name_fifth,'-999') != coalesce(c.dept_leader_name_fifth,'-999') or 

coalesce(h.dept_id_sixth,'-999') != coalesce(c.dept_id_sixth,'-999') or 
coalesce(h.dept_name_sixth,'-999') != coalesce(c.dept_name_sixth,'-999') or 
coalesce(h.dept_leader_id_sixth,'-999') != coalesce(c.dept_leader_id_sixth,'-999') or 
coalesce(h.dept_leader_name_sixth,'-999') != coalesce(c.dept_leader_name_sixth,'-999') or 

coalesce(h.dept_id_seventh,'-999') != coalesce(c.dept_id_seventh,'-999') or 
coalesce(h.dept_name_seventh,'-999') != coalesce(c.dept_name_seventh,'-999') or 
coalesce(h.dept_leader_id_seventh,'-999') != coalesce(c.dept_leader_id_seventh,'-999') or 
coalesce(h.dept_leader_name_seventh,'-999') != coalesce(c.dept_leader_name_seventh,'-999') or 

coalesce(h.dept_id_eighth,'-999') != coalesce(c.dept_id_eighth,'-999') or 
coalesce(h.dept_name_eighth,'-999') != coalesce(c.dept_name_eighth,'-999') or 
coalesce(h.dept_leader_id_eighth,'-999') != coalesce(c.dept_leader_id_eighth,'-999') or 
coalesce(h.dept_leader_name_eighth,'-999') != coalesce(c.dept_leader_name_eighth,'-999') 
) then regexp_replace(uuid(),'-','') -- join上并且有变化的数据行分配新主键 
when h.dept_id is null and c.dept_id is not null then regexp_replace(uuid(),'-','') -- 新增数据分配新主键 
else h.skey -- 保留原主键 
end as skey,

case when c.dept_id is not null then c.dept_id else h.dept_id end as dept_id,
case when c.dept_id is not null then c.dept_name else h.dept_name end as dept_name,
case when c.dept_id is not null then c.dept_level else h.dept_level end as dept_level,
case when c.dept_id is not null then c.team_leader_id else h.team_leader_id end as team_leader_id,
case when c.dept_id is not null then c.team_leader_name else h.team_leader_name end as team_leader_name,
case when c.dept_id is not null then c.city_id else h.city_id end as city_id,
case when c.dept_id is not null then c.city_name else h.city_name end as city_name,
case when c.dept_id is not null then c.cate_id else h.cate_id end as cate_id,
case when c.dept_id is not null then c.cate_name else h.cate_name end as cate_name,
case when c.dept_id is not null then c.dept_type_id else h.dept_type_id end as dept_type_id,
case when c.dept_id is not null then c.dept_type_name else h.dept_type_name end as dept_type_name,
case when c.dept_id is not null then c.dept_attr else h.dept_attr end as dept_attr,
case when c.dept_id is not null then c.dept_level_path else h.dept_level_path end as dept_level_path,
case when c.dept_id is not null then c.dept_level_leader else h.dept_level_leader end as dept_level_leader,

case when c.dept_id is not null then c.dept_id_first else h.dept_id_first end as dept_id_first,
case when c.dept_id is not null then c.dept_name_first else h.dept_name_first end as dept_name_first,
case when c.dept_id is not null then c.dept_leader_id_first else h.dept_leader_id_first end as dept_leader_id_first,
case when c.dept_id is not null then c.dept_leader_name_first else h.dept_leader_name_first end as dept_leader_name_first,

case when c.dept_id is not null then c.dept_id_second else h.dept_id_second end as dept_id_second,
case when c.dept_id is not null then c.dept_name_second else h.dept_name_second end as dept_name_second,
case when c.dept_id is not null then c.dept_leader_id_second else h.dept_leader_id_second end as dept_leader_id_second,
case when c.dept_id is not null then c.dept_leader_name_second else h.dept_leader_name_second end as dept_leader_name_second,

case when c.dept_id is not null then c.dept_id_third else h.dept_id_third end as dept_id_third,
case when c.dept_id is not null then c.dept_name_third else h.dept_name_third end as dept_name_third,
case when c.dept_id is not null then c.dept_leader_id_third else h.dept_leader_id_third end as dept_leader_id_third,
case when c.dept_id is not null then c.dept_leader_name_third else h.dept_leader_name_third end as dept_leader_name_third,

case when c.dept_id is not null then c.dept_id_fourth else h.dept_id_fourth end as dept_id_fourth,
case when c.dept_id is not null then c.dept_name_fourth else h.dept_name_fourth end as dept_name_fourth,
case when c.dept_id is not null then c.dept_leader_id_fourth else h.dept_leader_id_fourth end as dept_leader_id_fourth,
case when c.dept_id is not null then c.dept_leader_name_fourth else h.dept_leader_name_fourth end as dept_leader_name_fourth,

case when c.dept_id is not null then c.dept_id_fifth else h.dept_id_fifth end as dept_id_fifth,
case when c.dept_id is not null then c.dept_name_fifth else h.dept_name_fifth end as dept_name_fifth,
case when c.dept_id is not null then c.dept_leader_id_fifth else h.dept_leader_id_fifth end as dept_leader_id_fifth,
case when c.dept_id is not null then c.dept_leader_name_fifth else h.dept_leader_name_fifth end as dept_leader_name_fifth,

case when c.dept_id is not null then c.dept_id_sixth else h.dept_id_sixth end as dept_id_sixth,
case when c.dept_id is not null then c.dept_name_sixth else h.dept_name_sixth end as dept_name_sixth,
case when c.dept_id is not null then c.dept_leader_id_sixth else h.dept_leader_id_sixth end as dept_leader_id_sixth,
case when c.dept_id is not null then c.dept_leader_name_sixth else h.dept_leader_name_sixth end as dept_leader_name_sixth,

case when c.dept_id is not null then c.dept_id_seventh else h.dept_id_seventh end as dept_id_seventh,
case when c.dept_id is not null then c.dept_name_seventh else h.dept_name_seventh end as dept_name_seventh,
case when c.dept_id is not null then c.dept_leader_id_seventh else h.dept_leader_id_seventh end as dept_leader_id_seventh,
case when c.dept_id is not null then c.dept_leader_name_seventh else h.dept_leader_name_seventh end as dept_leader_name_seventh,

case when c.dept_id is not null then c.dept_id_eighth else h.dept_id_eighth end as dept_id_eighth,
case when c.dept_id is not null then c.dept_name_eighth else h.dept_name_eighth end as dept_name_eighth,
case when c.dept_id is not null then c.dept_leader_id_eighth else h.dept_leader_id_eighth end as dept_leader_id_eighth,
case when c.dept_id is not null then c.dept_leader_name_eighth else h.dept_leader_name_eighth end as dept_leader_name_eighth,

case when c.dept_id is not null then c.create_date else h.create_date end as create_date,

if(c.dept_id is null,1,0) as is_archived,

-- version 计算:
case 
when h.dept_id is not null and c.dept_id is not null and ( 
coalesce(h.dept_name,'-999') != coalesce(c.dept_name,'-999') or 
coalesce(h.team_leader_id,'-999') != coalesce(c.team_leader_id,'-999') or 
coalesce(h.team_leader_name,'-999') != coalesce(c.team_leader_name,'-999') or 
coalesce(h.city_id,'-999') != coalesce(c.city_id,'-999') or 
coalesce(h.cate_id,'-999') != coalesce(c.cate_id,'-999') or 
coalesce(h.dept_type_id,'-999') != coalesce(c.dept_type_id,'-999') or 
coalesce(h.dept_level_path,'-999') != coalesce(c.dept_level_path,'-999') or 

coalesce(h.dept_id_first,'-999') != coalesce(c.dept_id_first,'-999') or 
coalesce(h.dept_name_first,'-999') != coalesce(c.dept_name_first,'-999') or 
coalesce(h.dept_leader_id_first,'-999') != coalesce(c.dept_leader_id_first,'-999') or 
coalesce(h.dept_leader_name_first,'-999') != coalesce(c.dept_leader_name_first,'-999') or 

coalesce(h.dept_id_second,'-999') != coalesce(c.dept_id_second,'-999') or 
coalesce(h.dept_name_second,'-999') != coalesce(c.dept_name_second,'-999') or 
coalesce(h.dept_leader_id_second,'-999') != coalesce(c.dept_leader_id_second,'-999') or 
coalesce(h.dept_leader_name_second,'-999') != coalesce(c.dept_leader_name_second,'-999') or 

coalesce(h.dept_id_third,'-999') != coalesce(c.dept_id_third,'-999') or  
coalesce(h.dept_name_third,'-999') != coalesce(c.dept_name_third,'-999') or  
coalesce(h.dept_leader_id_third,'-999') != coalesce(c.dept_leader_id_third,'-999') or  
coalesce(h.dept_leader_name_third,'-999') != coalesce(c.dept_leader_name_third,'-999') or  

coalesce(h.dept_id_fourth,'-999') != coalesce(c.dept_id_fourth,'-999') or 
coalesce(h.dept_name_fourth,'-999') != coalesce(c.dept_name_fourth,'-999') or  
coalesce(h.dept_leader_id_fourth,'-999') != coalesce(c.dept_leader_id_fourth,'-999') or 
coalesce(h.dept_leader_name_fourth,'-999') != coalesce(c.dept_leader_name_fourth,'-999') or  

coalesce(h.dept_id_fifth,'-999') != coalesce(c.dept_id_fifth,'-999') or 
coalesce(h.dept_name_fifth,'-999') != coalesce(c.dept_name_fifth,'-999') or 
coalesce(h.dept_leader_id_fifth,'-999') != coalesce(c.dept_leader_id_fifth,'-999') or 
coalesce(h.dept_leader_name_fifth,'-999') != coalesce(c.dept_leader_name_fifth,'-999') or 

coalesce(h.dept_id_sixth,'-999') != coalesce(c.dept_id_sixth,'-999') or 
coalesce(h.dept_name_sixth,'-999') != coalesce(c.dept_name_sixth,'-999') or 
coalesce(h.dept_leader_id_sixth,'-999') != coalesce(c.dept_leader_id_sixth,'-999') or 
coalesce(h.dept_leader_name_sixth,'-999') != coalesce(c.dept_leader_name_sixth,'-999') or 

coalesce(h.dept_id_seventh,'-999') != coalesce(c.dept_id_seventh,'-999') or 
coalesce(h.dept_name_seventh,'-999') != coalesce(c.dept_name_seventh,'-999') or 
coalesce(h.dept_leader_id_seventh,'-999') != coalesce(c.dept_leader_id_seventh,'-999') or 
coalesce(h.dept_leader_name_seventh,'-999') != coalesce(c.dept_leader_name_seventh,'-999') or 

coalesce(h.dept_id_eighth,'-999') != coalesce(c.dept_id_eighth,'-999') or 
coalesce(h.dept_name_eighth,'-999') != coalesce(c.dept_name_eighth,'-999') or 
coalesce(h.dept_leader_id_eighth,'-999') != coalesce(c.dept_leader_id_eighth,'-999') or 
coalesce(h.dept_leader_name_eighth,'-999') != coalesce(c.dept_leader_name_eighth,'-999') 
) then h.version + 1  -- join上并且有变化的数据行版本号+1 
when h.dept_id is null and c.dept_id is not null then 1 -- 新增数据版本为1 
else h.version -- 版本没有变化
end as version,

1 as status,

-- start_date 计算:
case 
when h.dept_id is not null and c.dept_id is not null and ( 
coalesce(h.dept_name,'-999') != coalesce(c.dept_name,'-999') or 
coalesce(h.team_leader_id,'-999') != coalesce(c.team_leader_id,'-999') or 
coalesce(h.team_leader_name,'-999') != coalesce(c.team_leader_name,'-999') or 
coalesce(h.city_id,'-999') != coalesce(c.city_id,'-999') or 
coalesce(h.cate_id,'-999') != coalesce(c.cate_id,'-999') or 
coalesce(h.dept_type_id,'-999') != coalesce(c.dept_type_id,'-999') or 
coalesce(h.dept_level_path,'-999') != coalesce(c.dept_level_path,'-999') or 

coalesce(h.dept_id_first,'-999') != coalesce(c.dept_id_first,'-999') or 
coalesce(h.dept_name_first,'-999') != coalesce(c.dept_name_first,'-999') or 
coalesce(h.dept_leader_id_first,'-999') != coalesce(c.dept_leader_id_first,'-999') or 
coalesce(h.dept_leader_name_first,'-999') != coalesce(c.dept_leader_name_first,'-999') or 

coalesce(h.dept_id_second,'-999') != coalesce(c.dept_id_second,'-999') or 
coalesce(h.dept_name_second,'-999') != coalesce(c.dept_name_second,'-999') or 
coalesce(h.dept_leader_id_second,'-999') != coalesce(c.dept_leader_id_second,'-999') or 
coalesce(h.dept_leader_name_second,'-999') != coalesce(c.dept_leader_name_second,'-999') or 

coalesce(h.dept_id_third,'-999') != coalesce(c.dept_id_third,'-999') or  
coalesce(h.dept_name_third,'-999') != coalesce(c.dept_name_third,'-999') or  
coalesce(h.dept_leader_id_third,'-999') != coalesce(c.dept_leader_id_third,'-999') or  
coalesce(h.dept_leader_name_third,'-999') != coalesce(c.dept_leader_name_third,'-999') or  

coalesce(h.dept_id_fourth,'-999') != coalesce(c.dept_id_fourth,'-999') or 
coalesce(h.dept_name_fourth,'-999') != coalesce(c.dept_name_fourth,'-999') or  
coalesce(h.dept_leader_id_fourth,'-999') != coalesce(c.dept_leader_id_fourth,'-999') or 
coalesce(h.dept_leader_name_fourth,'-999') != coalesce(c.dept_leader_name_fourth,'-999') or  

coalesce(h.dept_id_fifth,'-999') != coalesce(c.dept_id_fifth,'-999') or 
coalesce(h.dept_name_fifth,'-999') != coalesce(c.dept_name_fifth,'-999') or 
coalesce(h.dept_leader_id_fifth,'-999') != coalesce(c.dept_leader_id_fifth,'-999') or 
coalesce(h.dept_leader_name_fifth,'-999') != coalesce(c.dept_leader_name_fifth,'-999') or 

coalesce(h.dept_id_sixth,'-999') != coalesce(c.dept_id_sixth,'-999') or 
coalesce(h.dept_name_sixth,'-999') != coalesce(c.dept_name_sixth,'-999') or 
coalesce(h.dept_leader_id_sixth,'-999') != coalesce(c.dept_leader_id_sixth,'-999') or 
coalesce(h.dept_leader_name_sixth,'-999') != coalesce(c.dept_leader_name_sixth,'-999') or 

coalesce(h.dept_id_seventh,'-999') != coalesce(c.dept_id_seventh,'-999') or 
coalesce(h.dept_name_seventh,'-999') != coalesce(c.dept_name_seventh,'-999') or 
coalesce(h.dept_leader_id_seventh,'-999') != coalesce(c.dept_leader_id_seventh,'-999') or 
coalesce(h.dept_leader_name_seventh,'-999') != coalesce(c.dept_leader_name_seventh,'-999') or 

coalesce(h.dept_id_eighth,'-999') != coalesce(c.dept_id_eighth,'-999') or 
coalesce(h.dept_name_eighth,'-999') != coalesce(c.dept_name_eighth,'-999') or 
coalesce(h.dept_leader_id_eighth,'-999') != coalesce(c.dept_leader_id_eighth,'-999') or 
coalesce(h.dept_leader_name_eighth,'-999') != coalesce(c.dept_leader_name_eighth,'-999') 
) then ${hiveconf:etl_date}  -- join上并且有变化的数据行 开始日期是当前日期 
when h.dept_id is null and c.dept_id is not null then c.create_date -- 新增数据日期 
else h.start_date -- 版本没有变化
end as start_date,

current_timestamp() as etl_time,
${hiveconf:end_date} as end_date 
;


