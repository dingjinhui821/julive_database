source /etc/profile
source ~/.bash_profile
for((i=283;i>=1;i--))
do
echo $i
dateID=$(date -d "-$i day" +"%Y-%m-%d")

echo "Input parameter:dateID="$dateID

/usr/bin/hive -hiveconf etldate=$dateID -f /data4/nfsdata/julive_dw/etl/sql/julive_fact/fact_subscribe_sign_payment_indi.sql
/usr/bin/hive -hiveconf etldate=$dateID -f /data4/nfsdata/julive_dw/etl/sql/julive_fact/fact_subscribe_no_sign_indi.sql
/usr/bin/hive -hiveconf etldate=$dateID -f /data4/nfsdata/julive_dw/etl/sql/julive_fact/fact_subscribe_no_sign_nowsign_indi.sql


EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
ssh -p10001 test01 <<eeooff
python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m "@15738713772: etl表(刷数脚本发生异常，请尽快处理!"
eeooff
    exit $EXCODE
fi
done

