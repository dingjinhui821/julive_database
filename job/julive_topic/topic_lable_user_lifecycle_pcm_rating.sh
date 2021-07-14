#!/bin/bash
source /etc/profile
source ~/.bash_profile

if [[ $# == 1 ]];then
    dateID=$1
elif [[ $# == 0 ]];then
    dateID=$(date -d "-1 day" +"%Y%m%d")
else
    echo "Usage sh "$0" dateID or sh "$0
    exit 1
fi


#1 计算日期 
etlTomorrow=$(date -d "${dateID} 1 days" +"%Y%m%d")
etlYestoday=$(date -d "${dateID} 1 days ago" +"%Y%m%d")

echo 日期参数: ${dateID} ${etlTomorrow} ${etlYestoday} 


#2、配置任务信息：sql文件路径和日志文件路径（日志文件需要通过日期时间配置唯一文件名）
sqlFile="/data4/nfsdata/julive_dw/etl/sql/julive_topic/topic_lable_user_lifecycle_pcm_rating.sql"
logFile="/data4/nfsdata/logs/etl/julive_topic/topic_lable_user_lifecycle_pcm_rating${sep}_${dateID}_${RANDOM}.log"

echo "输出日期文件路径: "${logFile}
echo "开始执行脚本: hive -hiveconf etlDate=${dateID} -hiveconf etlYestoday=${etlYestoday} -hiveconf etlTomorrow=${etlTomorrow} -f ${sqlFile}"
hive -hiveconf etlDate=${dateID} -hiveconf etlYestoday=${etlYestoday} -hiveconf etlTomorrow=${etlTomorrow} -f ${sqlFile} 2> ${logFile} 

EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
ssh -p10001 test01 <<eeooff
python /data4/nfsdata/julive_dw/script/python/market_alert_dd.py -m "@15738713772: etl表(topic_lable_user_lifecycle_pcm_rating)ETL发生异常，请尽快处理!"
eeooff
    exit $EXCODE
fi

