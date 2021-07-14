#!/bin/bash
source /etc/profile
source ~/.bash_profile

if [[ $# == 2 ]];then
    jobID=$1
    dateID=$2
elif [[ $# == 1 ]];then
    jobID=$1
    dateID=$(date -d "-1 day" +"%Y%m%d")
elif [[ $# == 0 ]];then
    dateID=$(date -d "-1 day" +"%Y%m%d")
else
    echo "Usage \"sh "$0" \${jobID} \${etl_date}\""
    echo "Usage \"sh "$0" \${jobID}\""
    echo "Usage \"sh "$0""
    exit 1
fi
cd $(dirname "$0")

# 1 定义预警接收人信息
monitorTaskName="fact_event_track_agg"
monitorGroup="etl"
monitorPhone="15998885390"

# 2 定义sqlFile和logFile文件路径地址
sqlFile="/data4/nfsdata/julive_dw/etl/sql/julive_fact/fact_event_track_agg.sql"
logFile="/data4/nfsdata/logs/etl/julive_fact/fact_event_track_agg${sep}_${dateID}.log"

/data4/nfsdata/dm/apps/anaconda3/bin/python /data4/nfsdata/julive_dw/bin/start.py \
jobID=${jobID} \
dateID=${dateID} \
${monitorTaskName},${monitorGroup},${monitorPhone} \
${sqlFile} \
${logFile}
