#!/bin/bash
#source /data1/etl/.bash_profile
source /etc/profile

DATE_ID=$(date -d "yesterday" +%Y%m%d )


DATE_ID_FORMAT=$(date -d "$DATE_ID" +%Y-%m-%d )

echo $DATE_ID_FORMAT

hdfs dfs -test -d /dw/dwd//
if [ $? -eq 0 ] ;then
    hdfs dfs -rm /dw/dwd//*
fi


hive -e ";"


EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
    ssh -p 10001 etl@test01 "python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m \"@18332118662, FAILED\""    
    exit $EXCODE
fi

