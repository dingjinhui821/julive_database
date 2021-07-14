#监控topic_sem_keyword_day_indi任务启动时间并处理
dateID=$(date -d "-1 day" +"%Y%m%d")
echo $dateID
cd /data4/nfsdata/julive_dw/etl/logs/julive_topic/
#获取当天最后一个生成的日志文件
filepath=$(find /data4/nfsdata/julive_dw/etl/logs/julive_topic/ -name 'topic_sem_keyword_day_indi_$dateID*' -type f |  xargs ls -t |head -n 1)
#截取日志文件名称
log_file=${filepath##*/}
echo "${log_file}"
#判断日志中任务执行第一个时间并截取
startdate=$(date -d "-0 day" +"%Y-%m-%d")
echo $startdate
starttime=$(find -type f -name $log_file | xargs grep "$startdate" | head -n 1)
#截取日志生成时间前8位
cut_time=${starttime:11:8}
 #将日志生成时间加上70分钟
execute_time=$(date -d " $cut_time 60 minute" +"%H:%M:%m")
 #判断任务执行时间与现在的关系
time=`date +%H:%M:%S`
if [[ "$time" > "$execute_time" ]]
then
#杀掉该任务
result=$(find -type f -name $log_file | xargs grep "application_" | head -n 1)
length=${result#*=}
echo "${length}"
/usr/bin/yarn application -kill $length
echo "********************************SUCCEED*********************************"
ssh -p10001 test01 <<eeooff
/data1/dm/apps/anaconda3/bin/python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m "@15738713772: etl表(topic_sem_keyword_day_indi)该任务70分钟前运行,已处理!"
eeooff
else
    echo "********************************FAILED WITH EXITCODE $EXCODE*********************************"
ssh -p10001 test01 <<eeooff
/data1/dm/apps/anaconda3/bin/python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m "@15738713772:etl表(topic_sem_keyword_day_indi)，未超过70分钟或未运行,请及时查看!"
eeooff
fi
