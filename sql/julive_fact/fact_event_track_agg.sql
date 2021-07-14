-- ---------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------

-- 1 schema : 事件分析底表 
-- drop table if exists julive_fact.fact_event_track_agg;
-- create table julive_fact.fact_event_track_agg(
-- 
-- global_id                 string  comment '用户唯一标识',
-- product_id                string  comment '端ID',
-- event                     string  comment '事件名称',
-- frompage                  string  comment '来源页面',
-- topage                    string  comment '目标页面',
-- pv                        bigint  comment '访问数'
-- 
-- ) comment '事件主维度指标表' 
-- partitioned by(pdate string)
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
-- monitorTaskName="fact_event_track_agg"
-- monitorGroup="etl"
-- monitorPhone="15998885390"
-- 
-- # 2 定义sqlFile和logFile文件路径地址
-- sqlFile="/data4/nfsdata/julive_dw/etl/sql/julive_fact/fact_event_track_agg.sql"
-- logFile="/data4/nfsdata/logs/etl/julive_fact/fact_event_track_agg${sep}_${dateID}.log"
-- 
-- /data4/nfsdata/dm/apps/anaconda3/bin/python /data4/nfsdata/julive_dw/bin/start.py \
-- jobID=${jobID} \
-- dateID=${dateID} \
-- ${monitorTaskName},${monitorGroup},${monitorPhone} \
-- ${sqlFile} \
-- ${logFile}
-- 


-- 3 etl 代码 
set etl_date = '${hiveconf:etlDate}'; 
set spark.app.name=fact_event_track_agg;
set mapred.job.name=fact_event_track_agg;
set spark.yarn.queue=etl;
set mapreduce.job.queuename=root.etl;

insert overwrite table julive_fact.fact_event_track_agg partition(pdate=${hiveconf:etl_date}) 

select 

global_id,
product_id,
event,
frompage,
topage,
count(1) as pv

from julive_fact.fact_event_dtl 

where pdate = ${hiveconf:etl_date} 

group by 
global_id,
product_id,
event,
frompage,
topage
;


