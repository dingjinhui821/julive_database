
set etl_date = '${hiveconf:etldate}'; -- 取昨天日期 
set etl_yestoday = '${hiveconf:etlyestoday}'; -- 取前天日期 

set hive.execution.engine=spark;
set mapreduce.job.queuename=root.etl;
set spark.app.name=events_metadata;
set spark.yarn.queue=etl;
set spark.default.parallelism=1400;
set spark.executor.cores=1;
set spark.executor.memory=4g;
set spark.executor.instances=14;
set spark.yarn.executor.memoryOverhead=4096;

set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set hive.prewarm.enabled=true;
set hive.prewarm.numcontainers=7;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

-- mysql 埋点字段元数据信息 元数据ETL
insert overwrite table tmp_dev_1.tmp_event_keys_mysql_metadata 

select 

lower(regexp_replace(trim(t1.value),' ',''))          as col_name,
max(upper(t1.name))                                   as col_comment,
min(t1.is_delete)                                     as is_delete

from julive_ods.uarp_roles t1 
where t1.type = 'extend'
and t1.value != '' 
and t1.value != 'a'
and t1.value != 'w'
and t1.value != '11'
group by t1.value 

union all 

select 

lower(trim(t1.type))                                         as col_name,
case 
when lower(t1.type) = lower('fromItemIndex') then '位置编号'
when lower(t1.type) = lower('fromItem') then '按钮' 
when lower(t1.type) = lower('toModule') then '到达模块' 
when lower(t1.type) = lower('fromModule') then '当前模块'
when lower(t1.type) = lower('fromPage') then '当前页面' 
when lower(t1.type) = lower('toPage') then '到达页面' 
end                                                          as col_comment,
0                                                            as is_delete 

from julive_ods.uarp_roles t1 
where t1.type in (
'fromItemIndex',
'fromItem',
'toModule',
'fromModule',
'fromPage',
'toPage'
)
group by t1.type 
;

-- 解析hive元数据ETL 
drop table if exists tmp_dev_1.tmp_event_keys_from_hive;
create table tmp_dev_1.tmp_event_keys_from_hive as 

select 

get_json_object(t1.json,"$.properties.product_id") as product_id,
get_json_object(t1.json,"$.event") as event,
default.get_json_keys(t1.json) as json_keys 

from ods.ods_events t1 
where t1.pdate = ${hiveconf:etl_date}
group by 

get_json_object(t1.json,"$.properties.product_id"),
get_json_object(t1.json,"$.event"),
default.get_json_keys(t1.json) 
;


with tmp as (
select 

t1.product_id,
lower(t1.event) as event,
lower(trim(col_name)) as col_name

from tmp_dev_1.tmp_event_keys_from_hive t1 
lateral view explode(split(t1.json_keys,",")) tmp_keys as col_name 
group by 
t1.product_id,
lower(t1.event),
lower(trim(col_name)) 
) 

insert overwrite table julive_dim.dict_event_keys_hive_metadata partition(pdate) 

select 

product_id,
event,
"" as first_col_name,
col_name,
${hiveconf:etl_date} as pdate 

from tmp 
where instr(col_name,".") = 0 

union all 

select 

product_id,
event,
split(col_name,"\\.")[0] as first_col_name,
split(col_name,"\\.")[1] as col_name,
${hiveconf:etl_date} as pdate 

from tmp 
where instr(col_name,".") > 0 
;


-- sqoop同步脚本：
-- sqoop export \
-- --connect jdbc:mysql://optimuspro01:3306/julive_dw \
-- --username root \
-- --password "QemENw>O0.Z" \
-- --export-dir hdfs://optimuspro01:8020/dw/julive_dim/dict_event_keys_result/pdate=20190724 \
-- --table dict_event_keys_result \
-- --columns first_col_name,col_name,col_comment,map_col_name,event_cnt,is_delete \
-- --fields-terminated-by '\001' \
-- --input-null-string '\\N' \
-- --input-null-non-string '\\N' \
-- --num-mappers 1 



-- 自动映射注释：
insert overwrite table julive_dim.dict_event_keys_result partition(pdate) 

select 

coalesce(t1.first_col_name,t2.first_col_name) as first_col_name,
coalesce(t1.col_name,t2.col_name) as col_name,
coalesce(t2.col_comment,t3.col_comment,t4.col_comment) as col_comment,
t2.map_col_name as map_col_name, 
t1.event_cnt,
case 
when t2.is_delete = 1 then 1 
when t3.is_delete = 1 then 1 
else 0 end as is_delete,
${hiveconf:etl_date} as pdate 

from (

select 

first_col_name,
col_name,
count(distinct event) as event_cnt 

from julive_dim.dict_event_keys_hive_metadata 
where pdate = ${hiveconf:etl_date} 
and event is not null 
and first_col_name != 'extractor'
and substr(col_name,1,1) != '_' 

group by 
first_col_name,
col_name 

) t1 
full join (
select * 
from julive_dim.dict_event_keys_result 
where pdate = ${hiveconf:etl_yestoday}  
) t2 on t1.col_name = t2.col_name 
left join tmp_dev_1.tmp_event_keys_mysql_metadata t3 on t1.col_name = t3.col_name 
left join julive_dim.dict_shence_cols_bak t4 on t1.col_name = t4.col_name 
order by t1.event_cnt desc  
;
