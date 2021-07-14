source /etc/profile
source ~/.bash_profile
for((i=201;i<=220;i++))
do
echo $i
dateID=$(date -d "-$i day" +"%Y-%m-%d")

echo "Input parameter:dateID="$dateID

/usr/bin/hive -hiveconf etldate=$dateID -f /data4/nfsdata/julive_dw/etl/sql/julive_fact/fact_subscribe_sign_payment_indi.sql

EXCODE=$?
if [ $EXCODE -eq 0 ]; then
echo "********************************SUCCEED*********************************"
else
echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
ssh -p10001 test01 <<eeooff
python /data4/nfsdata/dingtalk_monitor/monitor/bi_dd_alerter.py -m \ "@18001243001: etl表(fact_subscribe_sign_payment_indi)ETL发生异常，请尽快处理!"
eeooff
exit $EXCODE
fi
done
