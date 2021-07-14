-------------------------------------------------------------------------------------------
-- fact_event_dtl_v2 ----------------------------------------------------------------------
set etl_date = '${hiveconf:etldate}'; -- 取昨天日期 
set etl_tomorrow = '${hiveconf:etltomorrow}'; -- 取前天日期 

set hive.execution.engine=spark;
set spark.app.name=fact_event_esf_dtl;
set mapred.job.name=fact_event_esf_dtl;
set spark.yarn.queue=etl;
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

-- 2) 二手房项目 -----------------------------
-- pc端：session切割逻辑 5 分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.user_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.user_id order by t1.create_timestamp asc)) > 300000 -- 5分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.user_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.user_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.user_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.user_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.user_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.user_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_user_merged t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p31' -- 二手房项目 
and t1.product_id in (1,10,807,808) -- 1 新房 10 二手房 

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

tmp1.user_id as global_id,

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
user_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by user_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.user_id = tmp2.user_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;


-- m站：session切割逻辑 1 分钟 
with tmp as (

select 

if(lag(t1.create_timestamp)over(partition by t1.user_id order by t1.create_timestamp asc) is null or 
(t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.user_id order by t1.create_timestamp asc)) > 60000 -- 1分钟切割 
,regexp_replace(uuid(),'-',''),null
) as session_id,

round((t1.create_timestamp - lag(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.user_id order by t1.create_timestamp asc))/1000,4) as prev_event_elapse, -- 该事件与上一个事件耗时：秒
round((lead(t1.create_timestamp,1,t1.create_timestamp)over(partition by t1.user_id order by t1.create_timestamp asc) - t1.create_timestamp)/1000,4) as next_event_elapse, -- 该事件与下一个事件耗时：秒
-- row_number排序 
row_number()over(partition by t1.user_id order by t1.create_timestamp asc) as user_access_seq_asc_today,
row_number()over(partition by t1.user_id order by t1.create_timestamp desc) as user_access_seq_desc_today,
-- dense_rank排序 
dense_rank()over(partition by t1.user_id order by t1.create_timestamp asc) as user_access_seq_asc_today_dr,
dense_rank()over(partition by t1.user_id order by t1.create_timestamp desc) as user_access_seq_desc_today_dr,

t1.*

from tmp_dev_1.tmp_event_user_merged t1 
where t1.pdate = ${hiveconf:etl_date} 
and t1.pproject_type = 'p31' -- 二手房项目 
and t1.product_id in (2,20,807,808) -- 2 新房 20 二手房 

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

tmp1.user_id as global_id,

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
user_id,
user_access_seq_asc_today,
lead(user_access_seq_asc_today)over(partition by user_id order by create_timestamp asc) as user_access_seq_asc_today_next 

from tmp  
where session_id is not null 
) tmp2 on tmp1.user_id = tmp2.user_id 
where (tmp1.user_access_seq_asc_today >= tmp2.user_access_seq_asc_today)
and (tmp1.user_access_seq_asc_today < tmp2.user_access_seq_asc_today_next or tmp2.user_access_seq_asc_today_next is null)
;

