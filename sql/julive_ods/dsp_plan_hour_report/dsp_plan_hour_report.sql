set hive.execution.engine=spark;
set spark.app.name=dim_keyword_info;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table ods.dsp_plan_hour_report partition(pdate='${DATE_ID}')

select
id,                  
report_date,         
report_hour*100 as report_hour,         
media_type,         
account_id,          
account,             
plan_id,             
show_num,            
click_num,           
cost,                
bill_cost,           
creator,             
create_datetime,     
updator,             
update_datetime,     
current_timestamp()   as etl_time
from
tmp_dev_1.dsp_plan_hour_report_test where pdate = '${DATE_ID}';
