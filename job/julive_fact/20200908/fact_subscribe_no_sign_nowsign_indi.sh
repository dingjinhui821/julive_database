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
monitorTaskName="fact_subscribe_no_sign_nowsign_indi"
monitorGroup="etl"
monitorPhone="15738713772"

#2、配置任务信息：sql文件路径和日志文件路径（日志文件需要通过日期时间配置唯一文件名）
sqlFile="/data4/nfsdata/julive_dw/etl/sql/julive_fact/fact_subscribe_no_sign_nowsign_indi.sql"
logFile="/data4/nfsdata/julive_dw/etl/logs/julive_fact/fact_subscribe_no_sign_nowsign_indi${pre}_${dateID}.log"
echo "Output logFilePath:"${logFile}
/data4/nfsdata/dm/apps/anaconda3/bin/python /data4/nfsdata/julive_dw/bin/start.py ${jobID} ${dateID} ${monitorTaskName},${monitorGroup},${monitorPhone} ${sqlFile} ${logFile}

time=`date +%H:%M:%S`
echo $time
if [[ "$time" < "07:30:00" ]]
then
echo "********************************SUCCEED*********************************"
ssh -p10001 test01 <<eeooff
python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m "@15738713772: etl表(fact_subscribe_no_sign_nowsign_indi)运行成功 时间无异常!"
eeooff

else
    echo "********************************FAILED WITH EXITCODE $EXCODE*********************************"
ssh -p10001 test01 <<eeooff
python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m "@15738713772:etl表(fact_subscribe_no_sign_nowsign_indi)，运行时间有异常，请尽快处理!"
eeooff
 exit 0
fi

