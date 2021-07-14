#!/bin/bash
source /etc/profile
source ~/.bash_profile 

dateid=$(date -d "1 day ago" +"%Y%m%d")
yestoday=$(date -d "2 day ago" +"%Y%m%d")
echo "ETL DATE: $dateid $yestoday" 

hourid=$(date -d "1 day ago" +"%Y%m%d")

ssh -p10001 test01 "touch /data4/nfsdata/julive_dw/logs/etl/julive_app/app_xpt_data_complement_$hourid.log"
ssh -p10001 test01 "chmod 777 /data4/nfsdata/julive_dw/logs/etl/julive_app/app_xpt_data_complement_$hourid.log"
echo "running log:/data4/nfsdata/julive_dw/logs/etl/julive_app/app_xpt_data_complement_$hourid.log"

/data1/etl/streaming/spark-2.4.0-bin-hadoop2.6/bin/spark-submit \
--name mysql_hive_kafka \
--class com.julive.api.BatchDataSend \
--master master local[*] \
--driver-memory 2G \
--driver-cores 2 \
--num-executors 4 \
--executor-memory 4G \
--executor-cores 1  \
--queue root.etl \
--jars /data4/nfsdata/julive_dw/spark/jdp-batchdata-send-0.0.1-SNAPSHOT.jar \
/data4/nfsdata/julive_dw/spark/jdp-batchdata-send-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
'{"url":"jdbc:mysql://192.168.10.18:3306/julive_dw","username":"root","password":"O@DUXwg^PebmSHY!"}' 11 \
2>>/data4/nfsdata/julive_dw/logs/etl/julive_app/app_xpt_data_complement_$dateid.log

EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
ssh -p10001 test01 <<eeooff
python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m "@15738713772: etl表(app_xpt_data_complement tokafka)ETL发生异常，请尽快处理!"
eeooff
    exit $EXCODE
fi


