#!/bin/bash
source /etc/profile
source ~/.bash_profile

if [[ $# == 2 ]];then
    jobID=$1
    dateID=$2
elif [[ $# == 1 ]];then
    jobID=$1
    dateID=$(date -d "-1 day" +"%Y%m%d")
else
    echo "Usage \"sh "$0" \${jobID} \${etl_date}\""
    echo "Usage \"sh "$0" \${jobID}\""
    exit 1
fi

#1、配置告警信息：任务别名，执行用户(etl/bi/dm)，钉钉注册电话号码
monitorTaskName="tmp_market_dsp_sem_keyword_day_indi"
monitorGroup="etl"
monitorPhone="15738713772"

#2、配置任务信息：sql文件路径和日志文件路径（日志文件需要通过日期时间配置唯一文件名）
sqlFile="/data4/nfsdata/julive_dw/etl/sql/julive_topic/tmp_market_dsp_sem_keyword_day_indi_new.sql"
logFile="/data4/nfsdata/julive_dw/etl/logs/julive_topic/tmp_market_dsp_sem_keyword_day_indi${pre}_${dateID}.log"
echo "Output logFilePath:"${logFile}
/data4/nfsdata/dm/apps/anaconda3/bin/python /data4/nfsdata/julive_dw/bin/start.py ${jobID} ${dateID} ${monitorTaskName},${monitorGroup},${monitorPhone} ${sqlFile} ${logFile}

