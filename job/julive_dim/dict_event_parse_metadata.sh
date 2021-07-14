#!/bin/bash
source /etc/profile
source ~/.bash_profile 

if [[ $# == 2 ]];then
    date_id=$1
    date_id_yd=$2
elif [[ $# == 0 ]];then
    date_id=$(date -d "-1 day" +"%Y%m%d")
    date_id_yd=$(date -d "-2 day" +"%Y%m%d")
else
    echo "Usage \"sh "$0" date_id date_id_yd \" or \"sh "$0"\""
    exit 1
fi

cd $(dirname "$0")

echo "Input parameter:date_id="$date_id" date_id_yd="$date_id_yd

/usr/bin/hive -hiveconf etldate=$date_id -hiveconf etlyestoday=$date_id_yd -f ../../sql/julive_dim/dict_event_parse_metadata.sql


EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
ssh -p10001 test01 <<eeooff
python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m "@15738713772: etl表(app_hj_monitor_hour_dtl_impala)ETL发生异常，请尽快处理!"
eeooff
    exit $EXCODE
fi
