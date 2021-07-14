#!/bin/bash
source /etc/profile
#source ~/.bash_profile

/usr/bin/hive -f /data4/nfsdata/julive_dw/etl/sql/julive_app/app_short_url_info.sql


source /data1/dm/apps/anaconda3/bin/activate /data1/dm/apps/anaconda3/envs/py27
timeout 30 impala-shell -i optimuspro03:21000 -q "REFRESH julive_app.app_short_url_info;select count(1) from julive_app.app_short_url_info limit 10;"

#timeout 30 impala-shell -q "REFRESH julive_app.app_short_url_info;select count(1) from julive_app.app_short_url_info;"

EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
ssh -p10001 test01 <<eeooff
python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m "@15738713772: etl表(app_short_url_info)ETL发生异常，请尽快处理!"
eeooff
    exit $EXCODE
fi
