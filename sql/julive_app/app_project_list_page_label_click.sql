set etl_date = '${hiveconf:etlDate}'; -- 取昨天日期
set etl_tomorrow = '${hiveconf:etlTomorrow}'; -- 取前天日期
set hive.execution.engine=spark;
set spark.app.name=app_project_list_page_label_click;
set mapred.job.name=app_project_list_page_label_click;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table julive_app.app_project_list_page_label_click partition(pdate)

select 
   t1.select_city,
   t1.global_id,
   t1.label,
   current_timestamp()         as etl_time,
   t1.pdate 
   from
     (
      select  
       select_city,
       global_id,
       regexp_replace(get_json_object(properties,'$.project_type'),'\\[|\\]|\\"','') as label,
       pdate
     from julive_fact.fact_event_dtl
     where 
     pdate = ${hiveconf:etl_date} 
     and event ='e_filter_project'
     and topage='p_project_list'
     and get_json_object(properties,'$.comjia_platform_id')='2'
     and get_json_object(properties,'$.project_type') is not null
     and regexp_replace(get_json_object(properties,'$.project_type'),'\\[|\\]|\\"','')<>'0'
    
     union all 
    
     select  
        select_city,
        global_id,
        regexp_replace(get_json_object(properties,'$.sale_status'),'\\[|\\]|\\"','') as label,
        pdate
     from julive_fact.fact_event_dtl
     where 
     pdate = ${hiveconf:etl_date} 
     and event ='e_filter_project'
     and topage='p_project_list'
     and get_json_object(properties,'$.comjia_platform_id')='2'
     and get_json_object(properties,'$.sale_status') is not null
     and regexp_replace(get_json_object(properties,'$.sale_status'),'\\[|\\]|\\"','')<>'0'
    
     union all
    
     select  
       select_city,
       global_id,
       regexp_replace(get_json_object(properties,'$.brand_developer'),'\\[|\\]|\\"','') as label,
       pdate
     from julive_fact.fact_event_dtl
     where 
     pdate = ${hiveconf:etl_date}
     and event ='e_filter_project'
     and topage='p_project_list'
     and get_json_object(properties,'$.comjia_platform_id')='2'
     and get_json_object(properties,'$.brand_developer') is not null
     and regexp_replace(get_json_object(properties,'$.brand_developer'),'\\[|\\]|\\"','')<>'0'
    
     union all
    
     select
        select_city,
        global_id,
        regexp_replace(get_json_object(properties,'$.features'),'\\[|\\]|\\"','') as label,
        pdate
     from julive_fact.fact_event_dtl
     where 
     pdate = ${hiveconf:etl_date}
     and event ='e_filter_project'
     and topage='p_project_list'
     and get_json_object(properties,'$.comjia_platform_id')='2'
     and get_json_object(properties,'$.features') is not null
     and regexp_replace(get_json_object(properties,'$.features'),'\\[|\\]|\\"','')<>'0'
     )t1;

