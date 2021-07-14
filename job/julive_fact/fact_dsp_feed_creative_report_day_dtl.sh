#!/bin/bash
source /etc/profile
source ~/.bash_profile 

if [[ $# == 1 ]];then
    dateId=$1
elif [[ $# == 0 ]];then
    dateId=$(date -d "-1 day" +"%Y%m%d")
else
    echo "Usage \"sh "$0" dateId\" or \"sh "$0"\""
    exit 1
fi

cd $(dirname "$0")

echo "Input parameter:dateId="$dateId 
echo "Start exec "$0

/usr/bin/hive -hiveconf etldate=$dateId -f ../../sql/julive_fact/fact_dsp_feed_creative_report_day_dtl.sql

EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
ssh -p10001 test01 <<eeooff
python /data4/dw/dingtalk_monitor/monitor/azkaban_responder.py -m "@15738713772: etl表(fact_dsp_feed_creative_report_day_dtl)ETL发生异常，请尽快处理!"
eeooff
    exit $EXCODE
fi
