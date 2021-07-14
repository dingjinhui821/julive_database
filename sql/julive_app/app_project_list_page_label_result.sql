set etl_date = '${hiveconf:etlDate}'; -- 取昨天日期

set hive.execution.engine=spark;
set spark.app.name=app_project_list_page_label_result;
set mapred.job.name=app_project_list_page_label_result;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table julive_app.app_project_list_page_label_result
select 
  t1.select_city,
  t1.label,
  t1.label_uv,
  t2.total_uv,
  t1.label_uv/t2.total_uv       as user_scale,
  t1.user_sort                  as user_sort,
  current_timestamp()           as etl_time
  from(
  select  
    aa.select_city,
    aa.label,
    aa.label_uv,
    row_number() over (partition by aa.select_city order by aa.label_uv desc) as user_sort
    from 
      (
      select  
      a.select_city,
      a.label,
      count(distinct a.global_id) as label_uv
      from (
       select 
       select_city,
       global_id,
       label
       from
      julive_app.app_project_list_page_label_click
      where pdate>=from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-30),'yyyy-MM-dd'),'yyyyMMdd')
      and pdate<= ${hiveconf:etl_date}
      )a  
     group by a.select_city,a.label 
    )aa)t1 
   join
   ( 
    select  
      select_city,
      count(distinct global_id) as total_uv
    from julive_fact.fact_event_dtl
    where pdate>=from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-30),'yyyy-MM-dd'),'yyyyMMdd')
    and pdate<=${hiveconf:etl_date}
    and event ='e_filter_project'
    and topage='p_project_list'
    and get_json_object(properties,'$.comjia_platform_id')='2'
    and (get_json_object(properties,'$.project_type') is not null or get_json_object(properties,'$.sale_status') is not null
     or get_json_object(properties,'$.brand_developer') is not null or get_json_object(properties,'$.features') is not null) 
    group by select_city
   )t2  
   on t1.select_city=t2.select_city  
   where t2.total_uv >100  
   and t1.user_sort<=6;


