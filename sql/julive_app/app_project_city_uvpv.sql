set etl_date = '${hiveconf:etldate}';
set mapred.job.name=app_project_city_uvpv;
set mapreduce.job.queuename=root.etl;
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

insert overwrite table julive_app.app_project_city_uvpv partition(pdate) 

select 

project_id,
select_city,
global_id,
count(1) as pv,
pdate 

from julive_fact.fact_event_dtl 
where event='e_page_view' 
and toPage='p_project_home' 
and project_id != '' 

and pdate = ${hiveconf:etl_date} 

group by 
project_id,
select_city,
global_id,
pdate  
;



insert overwrite table julive_app.app_click_search_result_uvpv partition(pdate) 

select 

project_id,
select_city,
global_id,
count(1) as pv,
pdate 

from julive_fact.fact_event_dtl 
where event = 'e_click_search_result' 
and project_id != '' 

and pdate = ${hiveconf:etl_date} 

group by 
project_id,
select_city,
global_id,
pdate 
;

