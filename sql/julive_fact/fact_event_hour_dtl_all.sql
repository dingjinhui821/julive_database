
-- 补数etl ：
set etl_date = date_format(current_date(),'yyyyMMdd'); 
set etl_hour = from_unixtime((unix_timestamp(current_timestamp(),'yyyy-MM-dd HH:mm:ss')-3600),'yyyyMMdd-HH'); 

set hive.execution.engine=mr;
set spark.app.name=fact_event_dtl;
-- set hive.execution.engine=spark;
set spark.default.parallelism=1400;
set spark.executor.cores=4;
set spark.executor.memory=8g;
set spark.executor.instances=14;
set spark.yarn.executor.memoryOverhead=2048;
set hive.prewarm.enabled=true;
set hive.prewarm.numcontainers=14;
-- 
-- set hive.optimize.reducededuplication.min.reducer=4;
-- set hive.optimize.reducededuplication=true;
-- set hive.merge.mapfiles=true;
-- set hive.merge.mapredfiles=false;
-- set hive.merge.smallfiles.avgsize=16000000;
-- -- set hive.merge.size.per.task=256000000;
-- set hive.merge.sparkfiles=true;
-- set hive.auto.convert.join=false;
-- set hive.auto.convert.join.noconditionaltask=true;
-- set hive.auto.convert.join.noconditionaltask.size=209715200;
-- set hive.optimize.bucketmapjoin.sortedmerge=false;
-- set hive.map.aggr.hash.percentmemory=0.5;
-- set hive.map.aggr=true;
-- set hive.optimize.sort.dynamic.partition=false;
-- set hive.stats.autogather=true;
-- set hive.stats.fetch.column.stats=true;
-- set hive.compute.query.using.stats=true;
-- -- set hive.limit.pushdown.memory.usage=0.4;
-- set hive.optimize.index.filter=true;
-- set hive.exec.reducers.bytes.per.reducer=67108864;
-- set hive.smbjoin.cache.rows=10000;
-- set hive.fetch.task.conversion=more;
-- set hive.fetch.task.conversion.threshold=1073741824;
-- set hive.optimize.ppd=true;
-- set hive.mapjoin.localtask.max.memory.usage=0.999;
-- 
-- set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;
-------基础设置
set hive.exec.orc.default.block.size=134217728;
set mapreduce.input.fileinputformat.split.maxsize = 100000000;
set hive.auto.convert.join=true;
set hive.exec.compress.intermediate=true;
set hive.exec.reducers.bytes.per.reducer=500000000;
-----内存设置
set mapreduce.reduce.memory.mb=8192;
set mapreduce.map.memory.mb=8192;
set mapreduce.task.io.sort.mb=800;
-- set mapreduce.reduce.java.opts=-Djava.net.preferIPv4Stack=true -Xmx3200m;
set mapreduce.reduce.shuffle.parallelcopies=20;
set mapreduce.job.reduce.slowstart.completedmaps=0.8;
-----输出合并小文件
set hive.merge.mapfiles = true;
-- set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 128000000;
-- set hive.merge.smallfiles.avgsize=30000000;
-----输入合并小文件
set mapred.max.split.size=128000000; 
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.minsize.per.node=10000000;
set mapreduce.input.fileinputformat.split.minsize.per.rack=11000000;


with tmp_event_user_merged as ( 
select 

if(t2.global_id is not null,t2.global_id,t1.distinct_id) as global_id, -- global_id 问题 
t1.distinct_id,
t1.map_id,
t1.user_id,
t1.julive_id,
t1.comjia_unique_id,
t1.cookie_id,
t1.open_id,
t1.product_id,
t1.track_id,
t1.event,
t1.create_time,
t1.create_timestamp,
t1.recv_time,
t1.fl_project_id,
t1.fl_project_name,
t1.app_id,
t1.p_ip,
t1.idfa,
t1.idfv,
t1.channel_id,
t1.login_employee_id,
t1.project_id,
t1.comjia_android_id,
t1.comjia_device_id,
t1.comjia_imei,
t1.visitor_id,
t1.order_id,
t1.adviser_id,
t1.fromitemindex,
t1.fromitem,
t1.frompage,
t1.topage,
t1.frommodule,
t1.tomodule,
t1.p_is_first_day,
t1.p_is_first_time,
t1.app_version,
t1.p_app_version,
t1.select_city,
t1.lat,
t1.lng,
t1.current_url,
t1.to_url,
t1.referer,
t1.user_agent,
t1.p_utm_source,
t1.op_type,
t1.track_type,

-- 20191010新加 
t1.is_new_order,
t1.channel_put,
t1.login_state,
t1.button_title,
t1.banner_id,
t1.tab_id,
t1.leave_phone_state,
t1.source,
t1.operation_type,
t1.operation_position,
t1.abtest_name,
t1.abtest_value,

t1.lib,
t1.properties

from ( 

select 

get_json_object(t1.json,'$.distinct_id')                                                                     as distinct_id,
get_json_object(t1.json,'$.map_id')                                                                          as map_id,

cast(get_json_object(t1.json,'$.user_id') as bigint)                                                         as user_id, -- 废弃 

cast(cast(get_json_object(t1.json,'$.properties.julive_id') as decimal) as bigint)                           as julive_id,

get_json_object(t1.json,'$.properties.comjia_unique_id')                                                     as comjia_unique_id, -- 存在null 
get_json_object(t1.json,'$.properties.cookie_id')                                                            as cookie_id,
get_json_object(t1.json,'$.properties.open_id')                                                              as open_id, -- 存在null 
cast(floor(get_json_object(t1.json,'$.properties.product_id')) as decimal)                                   as product_id,

get_json_object(t1.json,'$.properties.utm_source')                                                           as utm_source, -- 瑞卓解析版本- 20191127前存储product_name,后存储utm_source 
get_json_object(t1.json,'$.properties.track_id')                                                             as track_id, -- 存在null 
get_json_object(t1.json,'$.event')                                                                           as event,
from_unixtime(cast(get_json_object(t1.json,'$.time')/1000 as bigint),'yyyy-MM-dd HH:mm:ss')                  as create_time,
get_json_object(t1.json,'$.time')                                                                            as create_timestamp,
from_unixtime(cast(get_json_object(t1.json,'$.recv_time')/1000 as bigint),'yyyy-MM-dd HH:mm:ss')             as recv_time,
get_json_object(t1.json,'$.project_id')                                                                      as fl_project_id,
get_json_object(t1.json,'$.project')                                                                         as fl_project_name,
cast(get_json_object(t1.json,'$.properties.app_id') as decimal)                                              as app_id, 
get_json_object(regexp_replace(t1.json,'\\$','p_'),'$.properties.p_ip')                                      as p_ip, -- 20191127更改取数方法 
get_json_object(t1.json,'$.properties.idfa')                                                                 as idfa, -- 存在null 
get_json_object(t1.json,'$.properties.IDFV')                                                                 as idfv, -- 存在null 
get_json_object(t1.json,'$.properties.channel_id')                                                           as channel_id,
get_json_object(t1.json,'$.properties.login_employee_id')                                                    as login_employee_id, -- 存在null 
get_json_object(t1.json,'$.properties.project_id')                                                           as project_id,
get_json_object(t1.json,'$.properties.comjia_android_id')                                                    as comjia_android_id, -- 存在null 
get_json_object(t1.json,'$.properties.comjia_device_id')                                                     as comjia_device_id, -- 存在null 

case 
when get_json_object(t1.json,'$.properties.comjia_imei') = '' 
or get_json_object(t1.json,'$.properties.comjia_imei') is null 
then get_json_object(t1.json,'$.properties.imei')
else get_json_object(t1.json,'$.properties.comjia_imei') 
end                                                                                                          as comjia_imei, -- 存在null 

get_json_object(t1.json,'$.properties.visitor_id')                                                           as visitor_id,
get_json_object(t1.json,'$.properties.order_id')                                                             as order_id, -- 存在null 
get_json_object(t1.json,'$.properties.adviser_id')                                                           as adviser_id, -- 存在null 

-- 第一版本 大小写 有问题 
get_json_object(t1.json,'$.properties.fromItemIndex')                                                        as fromitemindex,
get_json_object(t1.json,'$.properties.fromItem')                                                             as fromitem,
get_json_object(t1.json,'$.properties.fromPage')                                                             as frompage,
get_json_object(t1.json,'$.properties.toPage')                                                               as topage,
get_json_object(t1.json,'$.properties.fromModule')                                                           as frommodule,
get_json_object(t1.json,'$.properties.toModule')                                                             as tomodule,

get_json_object(regexp_replace(t1.json,'\\$','p_'),'$.properties.p_is_first_day')                            as p_is_first_day, -- 20191127更改取数方法 
get_json_object(regexp_replace(t1.json,'\\$','p_'),'$.properties.p_is_first_time')                           as p_is_first_time, -- 20191127更改取数方法 

get_json_object(t1.json,'$.properties.app_version')                                                          as app_version, -- 存在null 
get_json_object(regexp_replace(t1.json,'\\$','p_'),'$.properties.p_app_version')                             as p_app_version, -- 存在null -- 20191127更改取数方法 
get_json_object(t1.json,'$.properties.select_city')                                                          as select_city,
get_json_object(t1.json,'$.properties.lat')                                                                  as lat, -- 存在null 
get_json_object(t1.json,'$.properties.lng')                                                                  as lng, -- 存在null 
get_json_object(t1.json,'$.properties.current_url')                                                          as current_url,
get_json_object(t1.json,'$.properties.to_url')                                                               as to_url,  -- 存在null 
get_json_object(t1.json,'$.properties.referer')                                                              as referer,
get_json_object(t1.json,'$.properties.user_agent')                                                           as user_agent,
get_json_object(regexp_replace(t1.json,'\\$','p_'),'$.properties.p_utm_source')                              as p_utm_source, -- 存在null -- 20191127更改取数方法 
get_json_object(t1.json,'$.properties.op_type')                                                              as op_type, -- 存在null 

case 
when substr(get_json_object(json,'$.event'),1,2) = 'e_' then 1 
when substr(get_json_object(json,'$.event'),1,1) = '$' then 2 
else 3 end                                                                                                   as track_type,

-- 20191010新加属性  
get_json_object(t1.json,'$.properties.is_new_order')                                                         as is_new_order,
get_json_object(t1.json,'$.properties.channel_put')                                                          as channel_put,
get_json_object(t1.json,'$.properties.login_state')                                                          as login_state,
get_json_object(t1.json,'$.properties.button_title')                                                         as button_title,
get_json_object(t1.json,'$.properties.banner_id')                                                            as banner_id,
get_json_object(t1.json,'$.properties.tab_id')                                                               as tab_id,
get_json_object(t1.json,'$.properties.leave_phone_state')                                                    as leave_phone_state,
get_json_object(t1.json,'$.properties.source')                                                               as source,
get_json_object(t1.json,'$.properties.operation_type')                                                       as operation_type,
get_json_object(t1.json,'$.properties.operation_position')                                                   as operation_position,
get_json_object(t1.json,'$.properties.abtest_name')                                                          as abtest_name,
get_json_object(t1.json,'$.properties.abtest_value')                                                         as abtest_value,

-- json结构: 
get_json_object(t1.json,'$.lib')                                                                             as lib,
get_json_object(t1.json,'$.properties')                                                                      as properties

from julive_ods.ods_events_hour t1 -- 原始表 
where t1.phour <= ${hiveconf:etl_hour} 
and t1.phour >= concat(${hiveconf:etl_date},'-00')
and from_unixtime(cast(substr(get_json_object(t1.json,'$.time'),1,10) as bigint),'yyyyMMdd') = ${hiveconf:etl_date} -- time字段毫秒值 

-- 指定解析项目集合 
and cast(get_json_object(t1.json,'$.project_id') as int) = 5 

-- 剔除event null值数据
and get_json_object(t1.json,'$.event') != '' 
and get_json_object(t1.json,'$.event') is not null 
) t1 
left join julive_fact.global_id_exploded_hbase_2_hive t2 on case 
when (t1.comjia_unique_id is not null and t1.comjia_unique_id != '') then t1.comjia_unique_id 
when (t1.comjia_unique_id is null or t1.comjia_unique_id = '') and t1.product_id in (1,2) then t1.cookie_id 
when (t1.comjia_unique_id is null or t1.comjia_unique_id = '') and substr(t1.product_id,1,3) in (301,401) then t1.visitor_id 
else if(t1.distinct_id is not null and t1.distinct_id != '',t1.distinct_id,t1.user_id) 
end = t2.id 
) 

-- 剔除黑名单后版本 
insert overwrite table julive_fact.fact_event_hour_dtl partition(pdate,pplatform,phour) 

select 

"",
"",
"",
"",
"",
"",
"",
"",
tmp1.global_id,
tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
"" as product_name,
tmp1.track_id,
tmp1.event,
tmp1.create_time,
tmp1.create_timestamp,
tmp1.recv_time,
tmp1.fl_project_id,
tmp1.fl_project_name,
tmp1.app_id,
tmp1.p_ip,
tmp1.idfa,
tmp1.idfv,
tmp1.channel_id,
tmp1.login_employee_id,
tmp1.project_id,
tmp1.comjia_android_id,
tmp1.comjia_device_id,
tmp1.comjia_imei,
tmp1.visitor_id,
tmp1.order_id,
tmp1.adviser_id,
tmp1.fromitemindex,
tmp1.fromitem,
tmp1.frompage,
tmp1.topage,
tmp1.frommodule,
tmp1.tomodule,
tmp1.p_is_first_day,
tmp1.p_is_first_time,
tmp1.app_version,
tmp1.p_app_version,
tmp1.select_city,
tmp1.lat,
tmp1.lng,
tmp1.current_url,
tmp1.to_url,
tmp1.referer,
tmp1.user_agent,
tmp1.p_utm_source,
tmp1.op_type,
tmp1.track_type,
tmp1.is_new_order,
tmp1.channel_put,
tmp1.login_state,
tmp1.button_title,
tmp1.banner_id,
tmp1.tab_id,
tmp1.leave_phone_state,
tmp1.source,
tmp1.operation_type,
tmp1.operation_position,
tmp1.abtest_name,
tmp1.abtest_value,
tmp1.lib,
tmp1.properties,
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate, 
'all' as pplatform,
substr(tmp1.create_time,12,2) as phour 

from tmp_event_user_merged tmp1 
left join ( -- 找到 触发 黑名单数据 的 global_id 

select t1.global_id 
from (
select global_id,p_ip,comjia_imei,idfa 
from tmp_event_user_merged  
where global_id is not null 
group by global_id,p_ip,comjia_imei,idfa 
) t1 
left join ods.sys_sensors_black_ip t2 on t1.p_ip = t2.ip -- 排除黑名单数据 
left join dwd.dwd_black_imei t3 on t1.comjia_imei = t3.imei -- 排除imei刷量数据 
left join dwd.dwd_black_idfa t4 on t1.idfa = t4.idfa -- 排除idfa刷量数据 

where t2.id is not null 
   or t3.imei is not null 
   or t4.idfa is not null 
   
group by t1.global_id 

) tmp2 on tmp1.global_id = tmp2.global_id 
where tmp2.global_id is null 
;

