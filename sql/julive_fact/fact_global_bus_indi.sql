
-- ---------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------

-- 1 schema : 线下指标表 - global_id粒度 
-- drop table if exists julive_fact.fact_global_bus_indi;
-- create table julive_fact.fact_global_bus_indi(
-- global_id                 string  comment '主键',
-- julive_id                 string  comment '用户id',
-- user_name                 string  comment '用户名',
-- user_mobile               string  comment '用户手机号',
-- emp_id                    string  comment '员工id',
-- emp_name                  string  comment '员工姓名', 
-- create_date_min           string  comment '首次线索创建日期:yyyy-MM-dd',
-- create_date_max           string  comment '首次线索创建日期:yyyy-MM-dd',
-- distribute_date_min       string  comment '首次上户日期:yyyy-MM-dd',
-- distribute_date_max       string  comment '首次上户日期:yyyy-MM-dd',
-- first_see_date_min        string  comment '首次带看日期:yyyy-MM-dd',
-- first_see_date_max        string  comment '首次带看日期:yyyy-MM-dd',
-- first_subscribe_date_min  string  comment '首次认购日期:yyyy-MM-dd',
-- first_subscribe_date_max  string  comment '首次认购日期:yyyy-MM-dd',
-- first_sign_date_min       string  comment '首次签约日期:yyyy-MM-dd',
-- first_sign_date_max       string  comment '首次签约日期:yyyy-MM-dd',
-- clue_num                  bigint  comment '线索量',
-- distribute_num            bigint  comment '上户量',
-- see_num                   bigint  comment '带看量',
-- see_project_num           bigint  comment '带看楼盘量',
-- subscribe_num             bigint  comment '认购量:含退、含外联',
-- sign_num                  bigint  comment '签约量:含退、含外联',
-- call_duration             bigint  comment '线索通话时长(秒)',
-- call_num                  bigint  comment '线索通话次数',
-- clue_see_num              bigint  comment '产生带看的线索量',
-- clue_subscribe_num        bigint  comment '产生认购的线索量',
-- clue_sign_num             bigint  comment '产生签约的线索量',
-- etl_time                  string  comment 'ETL跑数时间' 
-- ) comment 'global_id粒度线下业务指标表' 
-- stored as parquet 
-- ;


-- 2 调度脚本 - Hera任务脚本配置模板：
-- #!/bin/bash
-- source /etc/profile
-- source ~/.bash_profile
-- 
-- if [[ $# == 2 ]];then
--     jobID=$1
--     dateID=$2
-- elif [[ $# == 1 ]];then
--     jobID=$1
--     dateID=$(date -d "-1 day" +"%Y%m%d")
-- elif [[ $# == 0 ]];then
--     dateID=$(date -d "-1 day" +"%Y%m%d")
-- else
--     echo "Usage \"sh "$0" \${jobID} \${etl_date}\""
--     echo "Usage \"sh "$0" \${jobID}\""
--     echo "Usage \"sh "$0""
--     exit 1
-- fi
-- cd $(dirname "$0")
-- 
-- # 1 定义预警接收人信息
-- monitorTaskName="fact_global_bus_indi"
-- monitorGroup="etl"
-- monitorPhone="15998885390"
-- 
-- # 2 定义sqlFile和logFile文件路径地址
-- sqlFile="/data4/nfsdata/julive_dw/etl/sql/julive_fact/fact_global_bus_indi.sql"
-- logFile="/data4/nfsdata/logs/etl/julive_fact/fact_global_bus_indi${sep}_${dateID}.log"
-- 
-- /data4/nfsdata/dm/apps/anaconda3/bin/python /data4/nfsdata/julive_dw/bin/start.py \
-- jobID=${jobID} \
-- dateID=${dateID} \
-- ${monitorTaskName},${monitorGroup},${monitorPhone} \
-- ${sqlFile} \
-- ${logFile}
-- 


-- 3 etl 代码 
set spark.app.name=fact_global_bus_indi;
set mapred.job.name=fact_global_bus_indi;
set spark.yarn.queue=etl;
insert overwrite table julive_fact.fact_global_bus_indi 

select 

t2.global_id                        as global_id,
t1.julive_id                        as julive_id,
max(t1.user_name)                   as user_name,
t1.user_mobile                      as user_mobile,
max(t1.emp_id)                      as emp_id,
max(t1.emp_name)                    as emp_name,
min(t1.create_date_min)             as create_date_min,
max(t1.create_date_max)             as create_date_max,
min(t1.distribute_date_min)         as distribute_date_min,
max(t1.distribute_date_max)         as distribute_date_max,
min(t1.first_see_date_min)          as first_see_date_min,
max(t1.final_see_date_max)          as first_see_date_max,
min(t1.first_subscribe_date_min)    as first_subscribe_date_min,
max(t1.final_subscribe_date_max)    as first_subscribe_date_max,
min(t1.first_sign_date_min)         as first_sign_date_min,
max(t1.final_sign_date_max)         as first_sign_date_max,
sum(t1.clue_num)                    as clue_num,
sum(t1.distribute_num)              as distribute_num,
sum(t1.see_num)                     as see_num,
sum(t1.see_project_num)             as see_project_num,
sum(t1.subscribe_num)               as subscribe_num,
sum(t1.sign_num)                    as sign_num,
sum(t1.call_duration)               as call_duration,
sum(t1.call_num)                    as call_num,
sum(t1.clue_see_num)                as clue_see_num,
sum(t1.clue_subscribe_num)          as clue_subscribe_num,
sum(t1.clue_sign_num)               as clue_sign_num,
current_timestamp()                 as etl_time 

from (
select 

t.user_id                     as julive_id,
max(t.user_name)              as user_name,
t.user_mobile                 as user_mobile,
max(t.emp_id)                 as emp_id,
max(t.emp_name)               as emp_name,

min(t.create_date)            as create_date_min,
max(t.create_date)            as create_date_max,
min(t.distribute_date)        as distribute_date_min,
max(t.distribute_date)        as distribute_date_max,
min(t.first_see_date)         as first_see_date_min,
max(t.final_see_date)         as final_see_date_max,
min(t.first_subscribe_date)   as first_subscribe_date_min,
max(t.final_subscribe_date)   as final_subscribe_date_max,
min(t.first_sign_date)        as first_sign_date_min,
max(t.final_sign_date)        as final_sign_date_max,
sum(t.clue_num)               as clue_num,
sum(t.distribute_num)         as distribute_num,
sum(t.see_num)                as see_num,
sum(t.see_project_num)        as see_project_num,
sum(t.subscribe_num)          as subscribe_num,
sum(t.sign_num)               as sign_num,
sum(t.call_duration)          as call_duration,
sum(t.call_num)               as call_num,
sum(t.clue_see_num)           as clue_see_num,
sum(t.clue_subscribe_num)     as clue_subscribe_num,
sum(t.clue_sign_num)          as clue_sign_num

from julive_fact.fact_clue_full_line_indi t 
where 1 = 1 
and t.create_date >= '2019-01-01' 
and t.user_id is not null -- bigint类型 
and t.user_id not in (0,-1) 

group by t.user_id,t.user_mobile 
) t1 join julive_fact.global_id_exploded_hbase_2_hive t2 on t1.julive_id = t2.id 

group by t2.global_id,t1.julive_id,t1.user_mobile
;


