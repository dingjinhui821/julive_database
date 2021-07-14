-- -----------------------------------------------------------------------
-- 0、hive运行参数设置 ---------------------------------------------------
-- -----------------------------------------------------------------------
set etl_date = '${hiveconf:etlDate}'; -- 取昨天日期 
set etl_tomorrow = '${hiveconf:etlTomorrow}'; -- 取前天日期 
-- 
-- set etl_date = '20200113'; -- 测试专用 
-- set etl_tomorrow = '20200114'; -- 测试专用 
-- 
set spark.app.name=fact_event_dtl;
set mapred.job.name=fact_event_dtl;
-- set hive.execution.engine=spark;
set mapreduce.job.queuename=root.etl;
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

--------------------------------------------------------------------------
-- 0、计算刷量数据 ------------------------------------------------------- 
--------------------------------------------------------------------------
insert overwrite table tmp_dev_1.tmp_event_brush_amount partition(pdate) 
select 

get_json_object(t1.json,'$.distinct_id') as distinct_id,
count(1) as cnt,
${hiveconf:etl_date} as pdate 
--from tmp_etl.tmp_fact_event_dtl_foremost_test t1
from ods.ods_events t1 
where t1.pdate = ${hiveconf:etl_date} 
--and get_json_object(regexp_replace(t1.json,'\\$','p_'),'$.properties.p_ip') !="117.148.126.176"
--and get_json_object(regexp_replace(t1.json,'\\$','p_'),'$.properties.p_ip') !="61.170.222.181"
--and get_json_object(regexp_replace(t1.json,'\\$','p_'),'$.properties.p_ip') !="112.23.77.212"
group by get_json_object(t1.json,'$.distinct_id') 
having count(1) > 50000 
;

-- -----------------------------------------------------------------------
-- 1、备份埋点采集数据,添加记录唯一标识 ---------------------------------- 
-- -----------------------------------------------------------------------
insert overwrite table julive_ods.ods_event partition(pdate,pproject_type) 

select 

regexp_replace(uuid(),"-","")                                                     as skey,
t1.json                                                                           as json,
${hiveconf:etl_date}                                                              as pdate,
case 
when cast(get_json_object(t1.json,'$.project_id') as int) = 5 then 'p05' 
when cast(get_json_object(t1.json,'$.project_id') as int) = 9 then 'p09' 
when cast(get_json_object(t1.json,'$.project_id') as int) = 31 then 'p31' 
when cast(get_json_object(t1.json,'$.project_id') as int) = 32 then 'p32' 
else 'p99' end                                                                    as pproject_type 
--from tmp_etl.tmp_fact_event_dtl_foremost_test t1
from ods.ods_events t1 
left join tmp_dev_1.tmp_event_brush_amount t2 on get_json_object(t1.json,'$.distinct_id') = t2.distinct_id and t2.pdate = ${hiveconf:etl_date}
where (t1.pdate = ${hiveconf:etl_date} or t1.pdate = ${hiveconf:etl_tomorrow}) 
and from_unixtime(cast(substr(get_json_object(t1.json,'$.time'),1,10) as bigint),'yyyyMMdd') = ${hiveconf:etl_date} -- time字段毫秒值 

-- 指定解析项目集合 
and cast(get_json_object(t1.json,'$.project_id') as int) in (5,9,31) 

-- 剔除event null值数据
and get_json_object(t1.json,'$.event') != '' 
and get_json_object(t1.json,'$.event') is not null 
and t2.distinct_id is null -- 排除刷量数据 
;
-- -----------------------------------------------------------------------
-- 2、解析临时表 --------------------------------------------------------- 
-- -----------------------------------------------------------------------


insert overwrite table tmp_dev_1.tmp_event_json_parsed partition(pdate,pproject_type) 

select 

t1.skey                                                                                                      as skey,
get_json_object(t1.json,'$.distinct_id')                                                                     as distinct_id,
get_json_object(t1.json,'$.map_id')                                                                          as map_id,

cast(get_json_object(t1.json,'$.user_id') as bigint)                                                         as user_id, -- 废弃 

-- get_json_object(t1.json,'$.properties.julive_id')                                                            as julive_id,
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
-- get_json_object(t1.json,'$.properties.app_id')                                                               as app_id, -- 存在null 
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
get_json_object(t1.json,'$.properties')                                                                      as properties, 
-- 以上顺序不用动 
${hiveconf:etl_date}                                                                                         as pdate, 
t1.pproject_type                                                                                             as pproject_type 


from julive_ods.ods_event t1 -- 替换ods.ods_events
where t1.pdate = ${hiveconf:etl_date} 
;


insert overwrite table julive_dim.dim_refrush_activate_balcklist_info partition(pdate)
select  
t3.p_ip                              as p_ip,
t3.p_model                           as p_model,
t3.comjia_unique_id                  as comjia_unique_id,
${hiveconf:etl_date}                 as pdate
from 
(select pdate,p_ip,get_json_object(properties,"$.model") as p_model,comjia_unique_id
from tmp_dev_1.tmp_event_json_parsed
where  pdate=${hiveconf:etl_date} 
and event='AppInstall'
and(product_name='' or product_name is null)
and get_json_object(properties,"$.channel")='appstore') t3
join 
(select t1.pdate as pdate ,t1.p_ip as p_ip ,t1.p_model as p_model
from 
(select pdate,p_ip,get_json_object(properties,"$.model") as p_model,count(distinct comjia_unique_id) as cnt
from tmp_dev_1.tmp_event_json_parsed
where  pdate=${hiveconf:etl_date} 
and event='AppInstall'
and (product_name='' or product_name is null)
and get_json_object(properties,"$.channel")='appstore'
group by pdate,p_ip,get_json_object(properties,"$.model") 
) t1
where t1.cnt>3
)t2  on t2.pdate=t3.pdate and t2.p_ip=t3.p_ip and t2.p_model=t3.p_model
;




-- 添加global_id 
insert overwrite table tmp_dev_1.tmp_event_user_merged partition(pdate,pproject_type) 

select 

t1.skey,
if(t2.global_id is not null,t2.global_id,t1.distinct_id) as global_id, -- global_id 问题 
t1.distinct_id,
t1.map_id,
t1.user_id,
t1.julive_id,
t1.comjia_unique_id,
t1.cookie_id,
t1.open_id,
t1.product_id,
t1.product_name,
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
t1.properties,
${hiveconf:etl_date} as pdate,
t1.pproject_type 

from (
select tmp1.* 
from tmp_dev_1.tmp_event_json_parsed tmp1 
where tmp1.pdate = ${hiveconf:etl_date}
and tmp1.comjia_unique_id not in (select comjia_unique_id from julive_dim.dim_refrush_activate_balcklist_info)
) t1 left join julive_fact.global_id_exploded_hbase_2_hive t2 on case 
when (t1.comjia_unique_id is not null and t1.comjia_unique_id != '') then t1.comjia_unique_id 
when (t1.comjia_unique_id is null or t1.comjia_unique_id = '') and t1.product_id in (1,2) then t1.cookie_id 
when (t1.comjia_unique_id is null or t1.comjia_unique_id = '') and substr(t1.product_id,1,3) in (301,401) then t1.visitor_id 
else if(t1.distinct_id is not null and t1.distinct_id != '',t1.distinct_id,t1.user_id) 
end = t2.id 
;



-- 分离黑名单及积分墙数据 
insert overwrite table tmp_dev_1.tmp_event_in_prd_div_blacklist partition(pdate,pproject_type,ptype) 

select 

tmp1.skey,
tmp1.global_id,
tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

tmp1.pdate,
tmp1.pproject_type,
if(tmp2.global_id is null,'p101','p102') as ptype -- 是否黑名单:p101 非黑名单 p102 黑名单

from (

select * 
from tmp_dev_1.tmp_event_user_merged  
where pdate = ${hiveconf:etl_date} 
and pproject_type in ('p05','p31') -- 正式项目、二手房项目 

) tmp1 left join ( -- 找到 触发 黑名单数据 的 global_id 

select t1.global_id 
from (
select global_id,p_ip,comjia_imei,idfa 
from tmp_dev_1.tmp_event_user_merged  
where pdate = ${hiveconf:etl_date} 
and pproject_type in ('p05','p31') 
and global_id is not null 
group by global_id,p_ip,comjia_imei,idfa 
) t1 
left join ods.sys_sensors_black_ip t2 on t1.p_ip = t2.ip -- 排除黑名单数据 
left join dwd.dwd_black_imei t3 on t1.comjia_imei = t3.imei -- 排除imei刷量数据 
left join 
     (select tmp1.idfa from (
      select idfa from dwd.dwd_black_idfa dwd_black
      --2021-07-01修改 ods.adc_active_from_third的数据已经在 949 对应调度的sql中合并到dwd.dwd_black_idfa表中了
      --union all
      --select idfa from ods.adc_active_from_third adc_active
      --where adc_active.idfa<>'00000000-0000-0000-0000-000000000000'
      --and adc_active.idfa !=''
      ) tmp1
      group by tmp1.idfa) t4 on t1.idfa = t4.idfa -- 排除idfa刷量数据 

where t2.id is not null 
   or t3.imei is not null 
   or t4.idfa is not null 
   
group by t1.global_id 

) tmp2 on tmp1.global_id = tmp2.global_id 
;


-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-- -----------------------------------------------------------------------
-- 3、分端切割session ---------------------------------------------------- 
-- ----------------------------------------------------------------------- 
set hive.execution.engine=spark;
set spark.default.parallelism=1400;
set spark.executor.cores=2;
set spark.executor.memory=4g;
set spark.executor.instances=14;
set spark.yarn.executor.memoryOverhead=4096;
set hive.prewarm.enabled=true;
set hive.prewarm.numcontainers=14;

set hive.optimize.reducededuplication.min.reducer=4;
set hive.optimize.reducededuplication=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;
set hive.merge.smallfiles.avgsize=16000000;
-- set hive.merge.size.per.task=256000000;
set hive.merge.sparkfiles=true;
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=209715200;
set hive.optimize.bucketmapjoin.sortedmerge=false;
set hive.map.aggr.hash.percentmemory=0.5;
set hive.map.aggr=true;
set hive.optimize.sort.dynamic.partition=false;
set hive.stats.autogather=true;
set hive.stats.fetch.column.stats=true;
set hive.compute.query.using.stats=true;
-- set hive.limit.pushdown.memory.usage=0.4;
set hive.optimize.index.filter=true;
set hive.exec.reducers.bytes.per.reducer=67108864;
set hive.smbjoin.cache.rows=10000;
set hive.fetch.task.conversion=more;
set hive.fetch.task.conversion.threshold=1073741824;
set hive.optimize.ppd=true;
set hive.mapjoin.localtask.max.memory.usage=0.999;

set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

-------基础设置
set hive.exec.orc.default.block.size=134217728;
set mapreduce.input.fileinputformat.split.maxsize = 100000000;
-- set hive.auto.convert.join=true;
-- set hive.exec.compress.intermediate=true;
-- set hive.exec.reducers.bytes.per.reducer=500000000;
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
-- -----------------------------------------
-- 1) 生产项目 -----------------------------
-- pc端：session切割逻辑 5 分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc)) > 300000 -- 5分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.global_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.global_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.global_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.global_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_in_prd_div_blacklist t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p05' -- 正式项目 
and t1.ptype = 'p101' -- 非黑名单数据 
and t1.product_id in (1) -- 1 pc新房 10 pc二手房 

)

insert overwrite table julive_fact.fact_event_dtl partition(pdate,pplatform)

select 

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加 
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

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'101' as pplatform 

from tmp tmp1 left join (
select 

session_id,
global_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by global_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.global_id = tmp2.global_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;


-- m站：session切割逻辑 1 分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc)) > 60000 -- 1分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.global_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.global_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.global_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.global_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_in_prd_div_blacklist t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p05' -- 正式项目 
and t1.ptype = 'p101' -- 非黑名单数据 
and t1.product_id in (2) -- 2 m新房 20 m二手房 

)

insert overwrite table julive_fact.fact_event_dtl partition(pdate,pplatform)

select 

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加 
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

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'102' as pplatform 

from tmp tmp1 left join (
select 

session_id,
global_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by global_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.global_id = tmp2.global_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;


-- app：session切割逻辑 3分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc)) > 180000 -- 3分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.global_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.global_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.global_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.global_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_in_prd_div_blacklist t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p05' -- 正式项目 
and t1.ptype = 'p101' 
and substr(t1.product_id,1,3) in (101,201) -- 101 安卓 201 ios 

)

insert overwrite table julive_fact.fact_event_dtl partition(pdate,pplatform)

select 

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加 
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

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'103' as pplatform 

from tmp tmp1 left join (
select 

session_id,
global_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by global_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.global_id = tmp2.global_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;



-- 小程序 ：session切割逻辑 1 分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc)) > 60000 -- 1分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.global_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.global_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.global_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.global_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_in_prd_div_blacklist t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p05' -- 正式项目 
and t1.ptype = 'p101' 
and substr(t1.product_id,1,3) in (301,401) -- 301 微信 401 百度 

)

insert overwrite table julive_fact.fact_event_dtl partition(pdate,pplatform)

select 

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加 
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

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'104' as pplatform 

from tmp tmp1 left join (
select 

session_id,
global_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by global_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.global_id = tmp2.global_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;


-- 其他埋点数据-不切割session 
insert overwrite table julive_fact.fact_event_dtl partition(pdate,pplatform) 

select 

tmp.skey,

'' as session_id,
'' as prev_event_elapse,
'' as next_event_elapse,
'' as user_access_seq_asc_today,
'' as user_access_seq_desc_today,
'' as user_access_seq_asc_today_dr,
'' as user_access_seq_desc_today_dr,

tmp.global_id,

tmp.distinct_id,
tmp.map_id,
tmp.user_id,
tmp.julive_id,
tmp.comjia_unique_id,
tmp.cookie_id,
tmp.open_id,
tmp.product_id,
tmp.product_name,
tmp.track_id,
tmp.event,
tmp.create_time,
tmp.create_timestamp,
tmp.recv_time,
tmp.fl_project_id,
tmp.fl_project_name,
tmp.app_id,
tmp.p_ip,
tmp.idfa,
tmp.idfv,
tmp.channel_id,
tmp.login_employee_id,
tmp.project_id,
tmp.comjia_android_id,
tmp.comjia_device_id,
tmp.comjia_imei,
tmp.visitor_id,
tmp.order_id,
tmp.adviser_id,
tmp.fromitemindex,
tmp.fromitem,
tmp.frompage,
tmp.topage,
tmp.frommodule,
tmp.tomodule,
tmp.p_is_first_day,
tmp.p_is_first_time,
tmp.app_version,
tmp.p_app_version,
tmp.select_city,
tmp.lat,
tmp.lng,
tmp.current_url,
tmp.to_url,
tmp.referer,
tmp.user_agent,
tmp.p_utm_source,
tmp.op_type,
tmp.track_type,

-- 20191010新加 
tmp.is_new_order,
tmp.channel_put,
tmp.login_state,
tmp.button_title,
tmp.banner_id,
tmp.tab_id,
tmp.leave_phone_state,
tmp.source,
tmp.operation_type,
tmp.operation_position,
tmp.abtest_name,
tmp.abtest_value,

tmp.lib,
tmp.properties,

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'999' as pplatform 

from tmp_dev_1.tmp_event_in_prd_div_blacklist tmp 
where tmp.pdate = ${hiveconf:etl_date} 
and tmp.pproject_type = 'p05' -- 正式项目 
and tmp.ptype = 'p101' 
and ((tmp.product_id not in (1,2) and substr(tmp.product_id,1,3) not in (101,201,301,401)) or (tmp.product_id is null)) 
;
--新平台product_id=7的数据
with tmp as (

select

if(lag(t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc) is null or
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc)) > 60000 -- 1分钟切割
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时>：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.global_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序
row_number()over(partition by t1.global_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.global_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序
dense_rank()over(partition by t1.global_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.global_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_in_prd_div_blacklist t1
where t1.pdate = ${hiveconf:etl_date}
and t1.pproject_type = 'p05' -- 正式项目
and t1.ptype = 'p101'
and substr(t1.product_id,1,3) in (7) -- 301 微信 401 百度

)

insert overwrite table julive_fact.fact_event_dtl partition(pdate,pplatform)

select

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加
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

-- 审计及分区字段
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'105' as pplatform

from tmp tmp1 left join (
select
session_id,
global_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by global_id order by create_timestamp asc) as user_access_seq_asc_today_next

from tmp
where session_id is not null
) tmp2 on tmp1.global_id = tmp2.global_id
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;


-------------------------------------------------------------------------------------------
-- fact_event_esf_dtl ---------------------------------------------------------------------
-- 2) 二手房项目 -----------------------------
-- pc端：session切割逻辑 5 分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc)) > 300000 -- 5分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_in_prd_div_blacklist t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p31' -- 二手房项目 
and t1.ptype = 'p101' -- 非黑名单数据 
and t1.product_id in (10) -- 1 新房 10 二手房 

)

insert overwrite table julive_fact.fact_event_esf_dtl partition(pdate,pplatform)

select 

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.distinct_id as global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加 
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

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'101' as pplatform 

from tmp tmp1 left join (
select 

session_id,
distinct_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by distinct_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.distinct_id = tmp2.distinct_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;


-- m站：session切割逻辑 1 分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc)) > 60000 -- 1分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_in_prd_div_blacklist t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p31' -- 二手房项目 
and t1.ptype = 'p101' -- 非黑名单数据 
and t1.product_id in (20) -- 2 新房 20 二手房 

)

insert overwrite table julive_fact.fact_event_esf_dtl partition(pdate,pplatform)

select 

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.distinct_id as global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加 
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

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'102' as pplatform 

from tmp tmp1 left join (
select 

session_id,
distinct_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by distinct_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.distinct_id = tmp2.distinct_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;



-- 新平台埋点：session切割逻辑 1 分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc)) > 60000 -- 1分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_in_prd_div_blacklist t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p31' -- 二手房项目 
and t1.ptype = 'p101' -- 非黑名单数据 
and t1.product_id = 801 -- 2 新房 20 二手房 801 新平台 

)

insert overwrite table julive_fact.fact_event_esf_dtl partition(pdate,pplatform)

select 

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.distinct_id as global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加 
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

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'103' as pplatform 

from tmp tmp1 left join (
select 

session_id,
distinct_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by distinct_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.distinct_id = tmp2.distinct_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;


-- product_id = 8 项目 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc)) > 60000 -- 1分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_in_prd_div_blacklist t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p31' -- 二手房项目 
and t1.ptype = 'p101' -- 非黑名单数据 
and t1.product_id in (8) -- 2 新房 20 二手房 

)

insert overwrite table julive_fact.fact_event_esf_dtl partition(pdate,pplatform)

select 

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.distinct_id as global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加 
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

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'104' as pplatform 

from tmp tmp1 left join (
select 

session_id,
distinct_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by distinct_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.distinct_id = tmp2.distinct_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;

-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-- 3) 咨询师项目 -----------------------------
-- 支撑系统：session切割逻辑 5 分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc)) > 300000 -- 5分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_user_merged t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p09' -- 咨询师项目 
and t1.product_id in (3) -- 支撑系统 3 

) 

insert overwrite table julive_fact.fact_event_emp_dtl partition(pdate,pplatform)

select 

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.distinct_id as global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加 
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

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'201' as pplatform 

from tmp tmp1 left join (
select 

session_id,
distinct_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by distinct_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.distinct_id = tmp2.distinct_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;


-- 咨询师app：session切割逻辑 3分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc)) > 180000 -- 3分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_user_merged t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p09' -- 咨询师项目 
and substr(t1.product_id,1,3) in (102) -- 咨询师app 102 

)

insert overwrite table julive_fact.fact_event_emp_dtl partition(pdate,pplatform)

select 

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.distinct_id as global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加 
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

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'202' as pplatform 

from tmp tmp1 left join (
select 

session_id,
distinct_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by distinct_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.distinct_id = tmp2.distinct_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;


-- 咨询师pad：session切割逻辑 3分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc)) > 180000 -- 3分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_user_merged t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p09' -- 咨询师项目 
and substr(t1.product_id,1,3) in (103,203) -- 咨询师pad 103 

)

insert overwrite table julive_fact.fact_event_emp_dtl partition(pdate,pplatform)

select 

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.distinct_id as global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加 
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

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'203' as pplatform 

from tmp tmp1 left join (
select 

session_id,
distinct_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by distinct_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.distinct_id = tmp2.distinct_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;


-- 慧剑系统：session切割逻辑 5分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc)) > 300000 -- 5分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_user_merged t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p09' -- 咨询师项目 
and t1.product_id in (40) -- 慧剑系统 40 

)

insert overwrite table julive_fact.fact_event_emp_dtl partition(pdate,pplatform)

select 

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.distinct_id as global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加 
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

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'204' as pplatform 

from tmp tmp1 left join (
select 

session_id,
distinct_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by distinct_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.distinct_id = tmp2.distinct_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;
--经纪人app 5分钟切割
with tmp as (

select

if(lag(t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) is null or
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc)) > 300000 -- 5分钟切割
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序
row_number()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_user_merged t1
where t1.pdate = ${hiveconf:etl_date}
and t1.pproject_type = 'p09' -- 咨询师项目
and t1.product_id in (807,808,809) -- 慧剑系统 40

)

insert overwrite table julive_fact.fact_event_emp_dtl partition(pdate,pplatform)

select

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.distinct_id as global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加
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

-- 审计及分区字段
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'205' as pplatform

from tmp tmp1 left join (
select
session_id,
distinct_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by distinct_id order by create_timestamp asc) as user_access_seq_asc_today_next

from tmp
where session_id is not null
) tmp2 on tmp1.distinct_id = tmp2.distinct_id
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;



--话务系统（esa）切割逻辑：5分钟
with tmp as (

select

if(lag(t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) is null or
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc)) > 300000 -- 5分钟切割
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序
row_number()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_user_merged t1
where t1.pdate = ${hiveconf:etl_date}
and t1.pproject_type = 'p09' -- 咨询师项目
and t1.product_id in (104) --话务系统

)

insert overwrite table julive_fact.fact_event_emp_dtl partition(pdate,pplatform)

select

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.distinct_id as global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加
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

-- 审计及分区字段
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'206' as pplatform

from tmp tmp1 left join (
select
session_id,
distinct_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by distinct_id order by create_timestamp asc) as user_access_seq_asc_today_next

from tmp
where session_id is not null
) tmp2 on tmp1.distinct_id = tmp2.distinct_id
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;




--人力系统切割逻辑：5分钟
with tmp as (

select

if(lag(t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) is null or
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc)) > 300000 -- 5分钟切割
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序
row_number()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_user_merged t1
where t1.pdate = ${hiveconf:etl_date}
and t1.pproject_type = 'p09' -- 咨询师项目
and t1.product_id in (34) --人力系统

)

insert overwrite table julive_fact.fact_event_emp_dtl partition(pdate,pplatform)

select

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.distinct_id as global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加
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

-- 审计及分区字段
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'207' as pplatform

from tmp tmp1 left join (
select
session_id,
distinct_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by distinct_id order by create_timestamp asc) as user_access_seq_asc_today_next

from tmp
where session_id is not null
) tmp2 on tmp1.distinct_id = tmp2.distinct_id
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;



--在线售楼处esa（自研机小米)切割逻辑：5分钟
with tmp as (

select

if(lag(t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) is null or
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc)) > 300000 -- 5分钟切割
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.distinct_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序
row_number()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.distinct_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_user_merged t1
where t1.pdate = ${hiveconf:etl_date}
and t1.pproject_type = 'p09' -- 咨询师项目
and t1.product_id in (705) --在线售楼处esa（自研机小米)

)

insert overwrite table julive_fact.fact_event_emp_dtl partition(pdate,pplatform)

select

tmp1.skey,

tmp2.session_id as session_id,
tmp1.prev_event_elapse,
tmp1.next_event_elapse,
tmp1.user_access_seq_asc_today,
tmp1.user_access_seq_desc_today,
tmp1.user_access_seq_asc_today_dr,
tmp1.user_access_seq_desc_today_dr,

tmp1.distinct_id as global_id,

tmp1.distinct_id,
tmp1.map_id,
tmp1.user_id,
tmp1.julive_id,
tmp1.comjia_unique_id,
tmp1.cookie_id,
tmp1.open_id,
tmp1.product_id,
tmp1.product_name,
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

-- 20191010新加
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

-- 审计及分区字段
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate,
'208' as pplatform

from tmp tmp1 left join (
select
session_id,
distinct_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by distinct_id order by create_timestamp asc) as user_access_seq_asc_today_next

from tmp
where session_id is not null
) tmp2 on tmp1.distinct_id = tmp2.distinct_id
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;




-- -------------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------------
-- 集成黑名单数据 ----------------------------------------------------------------------------------------
-- 
-- insert overwrite table julive_fact.fact_event_blacklist partition(pdate) 
-- select 
-- 
-- skey,
-- global_id,
-- distinct_id,
-- map_id,
-- user_id,
-- julive_id,
-- comjia_unique_id,
-- cookie_id,
-- open_id,
-- product_id,
-- product_name,
-- track_id,
-- event,
-- create_time,
-- create_timestamp,
-- recv_time,
-- fl_project_id,
-- fl_project_name,
-- app_id,
-- p_ip,
-- idfa,
-- idfv,
-- channel_id,
-- login_employee_id,
-- project_id,
-- comjia_android_id,
-- comjia_device_id,
-- comjia_imei,
-- visitor_id,
-- order_id,
-- adviser_id,
-- fromitemindex,
-- fromitem,
-- frompage,
-- topage,
-- frommodule,
-- tomodule,
-- p_is_first_day,
-- p_is_first_time,
-- app_version,
-- p_app_version,
-- select_city,
-- lat,
-- lng,
-- current_url,
-- to_url,
-- referer,
-- user_agent,
-- p_utm_source,
-- op_type,
-- track_type,
-- is_new_order,
-- channel_put,
-- login_state,
-- button_title,
-- banner_id,
-- tab_id,
-- leave_phone_state,
-- source,
-- operation_type,
-- operation_position,
-- abtest_name,
-- abtest_value,
-- lib,
-- properties,
-- current_timestamp() as etl_time,
-- pdate
-- 
-- from tmp_dev_1.tmp_event_user_merged t1 
-- where t1.pdate = ${hiveconf:etl_date} 
-- and t1.pproject_type = 'p-99' -- 黑名单数据 
-- ;
-- 
-- -------------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------------
-- 备份待刷新数据 
-- set spark.app.name=fact_event_dtl;
-- -- set hive.execution.engine=spark;
-- set spark.default.parallelism=1400;
-- set spark.executor.cores=4;
-- set spark.executor.memory=8g;
-- set spark.executor.instances=14;
-- set spark.yarn.executor.memoryOverhead=2048;
-- set hive.prewarm.enabled=true;
-- set hive.prewarm.numcontainers=14;
-- -- 
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- set hive.exec.max.dynamic.partitions=10000;
-- set hive.exec.max.dynamic.partitions.pernode=10000;
-- 
-- -------基础设置
-- set hive.exec.orc.default.block.size=134217728;
-- set mapreduce.input.fileinputformat.split.maxsize = 100000000;
-- set hive.auto.convert.join=true;
-- set hive.exec.compress.intermediate=true;
-- set hive.exec.reducers.bytes.per.reducer=500000000;
-- -----内存设置
-- set mapreduce.reduce.memory.mb=8192;
-- set mapreduce.map.memory.mb=8192;
-- set mapreduce.task.io.sort.mb=800;
-- -- set mapreduce.reduce.java.opts=-Djava.net.preferIPv4Stack=true -Xmx3200m;
-- set mapreduce.reduce.shuffle.parallelcopies=20;
-- set mapreduce.job.reduce.slowstart.completedmaps=0.8;
-- -----输出合并小文件
-- set hive.merge.mapfiles = true;
-- -- set hive.merge.mapredfiles = true;
-- set hive.merge.size.per.task = 128000000;
-- -- set hive.merge.smallfiles.avgsize=30000000;
-- -----输入合并小文件
-- set mapred.max.split.size=128000000; 
-- set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-- set mapreduce.input.fileinputformat.split.minsize.per.node=10000000;
-- set mapreduce.input.fileinputformat.split.minsize.per.rack=11000000;
-- 
-- 
-- insert overwrite table tmp_dev_1.tmp_fact_event_refresh_globalid partition(pdate) 
-- 
-- select 
-- 
-- skey,
-- session_id,
-- prev_event_elapse,
-- next_event_elapse,
-- user_access_seq_asc_today,
-- user_access_seq_desc_today,
-- user_access_seq_asc_today_dr,
-- user_access_seq_desc_today_dr,
-- global_id,
-- distinct_id,
-- map_id,
-- user_id,
-- julive_id,
-- comjia_unique_id,
-- cookie_id,
-- open_id,
-- product_id,
-- product_name,
-- track_id,
-- event,
-- create_time,
-- create_timestamp,
-- recv_time,
-- fl_project_id,
-- fl_project_name,
-- app_id,
-- p_ip,
-- idfa,
-- idfv,
-- channel_id,
-- login_employee_id,
-- project_id,
-- comjia_android_id,
-- comjia_device_id,
-- comjia_imei,
-- visitor_id,
-- order_id,
-- adviser_id,
-- fromitemindex,
-- fromitem,
-- frompage,
-- topage,
-- frommodule,
-- tomodule,
-- p_is_first_day,
-- p_is_first_time,
-- app_version,
-- p_app_version,
-- select_city,
-- lat,
-- lng,
-- current_url,
-- to_url,
-- referer,
-- user_agent,
-- p_utm_source,
-- op_type,
-- track_type,
-- is_new_order,
-- channel_put,
-- login_state,
-- button_title,
-- banner_id,
-- tab_id,
-- leave_phone_state,
-- source,
-- operation_type,
-- operation_position,
-- abtest_name,
-- abtest_value,
-- lib,
-- properties,
-- etl_time,
-- pplatform,
-- pdate 
-- 
-- from julive_fact.fact_event_dtl t1 
-- where t1.pdate <= ${hiveconf:etl_date} 
-- and t1.pdate >= regexp_replace(date_add(concat_ws('-',substr(${hiveconf:etl_date},1,4),substr(${hiveconf:etl_date},5,2),substr(${hiveconf:etl_date},7,2)),-31),'-','')
-- ;

quit;



