-- 增量ETL -------------------------------------------------------------------------------------------------
-- 增量ETL -------------------------------------------------------------------------------------------------
-- 增量ETL -------------------------------------------------------------------------------------------------
-- 增量ETL 
set etl_date = '${hiveconf:etldate}'; 
set etl_yestoday = '${hiveconf:etlyestoday}'; 

-- set etl_date = '20191125'; -- test 
-- set etl_yestoday = '20191124';  -- test  

set hive.execution.engine=spark;
set spark.yarn.queue=etl;
set spark.app.name=app_event_only_report_indi_final; 
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
set mapreduce.reduce.shuffle.parallelcopies=20;
set mapreduce.job.reduce.slowstart.completedmaps=0.8;
-----输出合并小文件
set hive.merge.mapfiles = true;
set hive.merge.size.per.task = 128000000;
-----输入合并小文件
set mapred.max.split.size=128000000; 
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.minsize.per.node=10000000;
set mapreduce.input.fileinputformat.split.minsize.per.rack=11000000;


-- 首电报告 
drop table if exists tmp_dev_1.tmp_app_event_only_report_indi_final_yestoday;
create table tmp_dev_1.tmp_app_event_only_report_indi_final_yestoday as 

select 

report_id,
total 

from julive_app.app_event_only_report_indi_final t1 
where ptype = 'backend_report_first_views' 
and pdate = ${hiveconf:etl_yestoday} 
;


insert overwrite table julive_app.app_event_only_report_indi_final partition(pdate,ptype) 

select 

report_id,
sum(total) as total,
${hiveconf:etl_date} as pdate,
'backend_report_first_views' as ptype 

from ( 
select 

report_id,
total 

from tmp_dev_1.tmp_app_event_only_report_indi_final_yestoday 

union all 

select 

report_id,
count(*) as total 

from julive_fact.fact_event_dtl_full_view 
where event='e_page_view' 
and toPage='p_report_first_details' 
and (open_source is null or open_source = 3)
and (page_soure='sms' or page_soure='wechat_service' or page_soure='miniprogram') 
and pdate = ${hiveconf:etl_date} 

group by report_id 
) t 
group by report_id 
;

-- 带看报告 
drop table if exists tmp_dev_1.tmp_app_event_only_report_indi_daikan_yestoday;
create table tmp_dev_1.tmp_app_event_only_report_indi_daikan_yestoday as 

select 

report_id,
total 

from julive_app.app_event_only_report_indi_final t1 
where ptype = 'backend_report_see_project_views' 
and pdate = ${hiveconf:etl_yestoday} 
;


insert overwrite table julive_app.app_event_only_report_indi_final partition(pdate,ptype) 

select 

report_id,
sum(total) as total,
${hiveconf:etl_date} as pdate,
'backend_report_see_project_views' as ptype 

from ( 
select 

report_id,
total 

from tmp_dev_1.tmp_app_event_only_report_indi_daikan_yestoday 

union all 

select

report_id,
count(*) as total

from julive_fact.fact_event_dtl_full_view 
where event='e_page_view'
and toPage='p_report_watching_details' 
and (open_source is null or open_source = 3)
and (page_soure='sms' or page_soure='wechat_service' or page_soure='miniprogram')
and pdate = ${hiveconf:etl_date} 

group by report_id 
) t 
group by report_id 
;


-- 认购报告 
drop table if exists tmp_dev_1.tmp_app_event_only_report_indi_rengou_yestoday;
create table tmp_dev_1.tmp_app_event_only_report_indi_rengou_yestoday as 

select 

report_id,
total 

from julive_app.app_event_only_report_indi_final t1 
where ptype = 'backend_subscribe_before_views' 
and pdate = ${hiveconf:etl_yestoday} 
;

insert overwrite table julive_app.app_event_only_report_indi_final partition(pdate,ptype) 

select 

t.report_id,
sum(total) as total,
${hiveconf:etl_date} as pdate,
'backend_subscribe_before_views' as ptype 

from ( 
select 

report_id,
total

from tmp_dev_1.tmp_app_event_only_report_indi_rengou_yestoday 

union all 

select

report_id,
count(*) as total

from julive_fact.fact_event_dtl_full_view 
where toPage='p_reminds_subscribe' 
and event='e_page_view'
and (page_soure='sms' or page_soure='wechat_service') 
and pdate = ${hiveconf:etl_date} 

group by report_id 
) t 
group by t.report_id 
;


