
-- refresh_fact_event_dtl_global_id_task

set etl_date = '${hiveconf:etldate}'; 

-- set etl_date = regexp_replace(date_add(current_date(),-1),'-','');

set hive.execution.engine=spark;
set spark.app.name=fact_event_dtl;
set mapred.job.name=fact_event_dtl;
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



insert overwrite table julive_fact.fact_event_dtl partition(pdate,pplatform) 

select 

t1.skey,
t1.session_id,
t1.prev_event_elapse,
t1.next_event_elapse,
t1.user_access_seq_asc_today,
t1.user_access_seq_desc_today,
t1.user_access_seq_asc_today_dr,
t1.user_access_seq_desc_today_dr,
t2.global_id,
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
t1.etl_time,
t1.pdate,
t1.pplatform 

from (
select * 
from tmp_dev_1.tmp_fact_event_refresh_globalid 
where pdate = ${hiveconf:etl_date} 
) t1 left join julive_fact.global_id_exploded_hbase_2_hive t2 on case 
when (t1.comjia_unique_id is not null and t1.comjia_unique_id != '') then t1.comjia_unique_id 
when (t1.comjia_unique_id is null or t1.comjia_unique_id = '') and t1.product_id in (1,2) then t1.cookie_id 
when (t1.comjia_unique_id is null or t1.comjia_unique_id = '') and substr(t1.product_id,1,3) in (301,401) then t1.visitor_id 
else if(t1.distinct_id is not null and t1.distinct_id != '',t1.distinct_id,t1.user_id) 
end = t2.id 
;

