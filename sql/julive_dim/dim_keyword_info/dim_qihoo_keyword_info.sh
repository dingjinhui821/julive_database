#!/bin/bash
#source /data1/etl/.bash_profile
source /etc/profile

DATE_ID=$(date -d "-1 day" +%Y%m%d )


DATE_ID_FORMAT=$(date -d "$DATE_ID" +%Y-%m-%d )

echo $DATE_ID_FORMAT

hive -hivevar DATE_ID=${DATE_ID} -f /data4/nfsdata/julive_dw/etl/sql/julive_dim/dim_keyword_info/dim_qihoo_keyword_info.sql

EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
    ssh -p 10001 etl@test01 "python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m \"@15738713772,julive_dim.dim_qihoo_keyword_info FAILED\""    
    exit $EXCODE
fi
