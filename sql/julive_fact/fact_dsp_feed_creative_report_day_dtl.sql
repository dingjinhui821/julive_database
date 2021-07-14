
set etl_date = '${hiveconf:etldate}';
-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 仅用于测试 

set hive.execution.engine=spark;
set spark.app.name=fact_dsp_feed_creative_report_day_dtl;
set mapred.job.name=fact_dsp_feed_creative_report_day_dtl;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table julive_fact.fact_dsp_feed_creative_report_day_dtl partition(psrc)

select 

t2.city_id,
t2.city_name,
t1.channel_id,
t2.channel_name,
t3.media_type as media_id,

case 
when t3.media_type = 1 then '百度' 
when t3.media_type = 2 then '360' 
when t3.media_type = 3 then '搜狗' 
when t3.media_type = 4 then '今日头条' 
when t3.media_type = 5 then '腾讯智汇推' 
when t3.media_type = 6 then '百度信息流' 
when t3.media_type = 7 then 'APP' 
when t3.media_type = 8 then '其他' 
when t3.media_type = 9 then '免费' 
when t3.media_type = 10 then '导航' 
when t3.media_type = 11 then '神马' 
when t3.media_type = 12 then '厂商' 
when t3.media_type = 13 then '微信' 
when t3.media_type = 14 then '端口' 
end as media_name,

t1.device as device_id,
case 
when t1.device = 1 then 'PC'
when t1.device = 2 then 'M'
end as device_name,

t1.dsp_account_id as account_id,
t1.account_name,
t1.plan_id,
t1.plan_name,
t1.unit_id,
t1.unit_name,

t1.creative_id,
t1.creative_name,
t1.title as creative_title,
t1.channel_put,
t1.bid_type,
from_unixtime(t1.report_date,'yyyy-MM-dd') as report_date,

t1.show_num,
t1.click_num,
t1.bill_cost,
t1.cost,
t1.price,
from_unixtime(t1.report_date,'yyyyMMdd') as pdate,
current_timestamp() as etl_time,

case 
when t3.media_type = 1 then 'baidu' 
when t3.media_type = 2 then '360' 
when t3.media_type = 3 then 'sougou' 
when t3.media_type = 4 then 'toutiao' 
when t3.media_type = 5 then 'tengxun' 
when t3.media_type = 6 then 'baiduxinxi' 
when t3.media_type = 11 then 'shenma' 
when t3.media_type = 13 then 'weixin' 
else 'others' end psrc 

from ods.dsp_creative_report t1 
left join julive_dim.dim_channel_info t2 on t1.channel_id = t2.channel_id 
left join ods.dsp_account t3 on t1.dsp_account_id = t3.id 
;



insert overwrite table julive_fact.fact_dsp_feed_creative_lkw_day_agg partition(psrc) 

select 

city_id,
city_name,
channel_id,
channel_name,
media_id,
media_name,
account_id,
account_name,
plan_id,
plan_name,
unit_id,
unit_name,
creative_id,
creative_name,
creative_title,
channel_put,
report_date,
sum(t1.show_num) as show_num,
sum(t1.click_num) as click_num,
sum(t1.cost) as cost,
t1.pdate,
current_timestamp() as etl_time,
t1.psrc 

from julive_fact.fact_dsp_feed_creative_report_day_dtl t1 
where t1.psrc != 'others'

group by 
city_id,
city_name,
channel_id,
channel_name,
media_id,
media_name,
account_id,
account_name,
plan_id,
plan_name,
unit_id,
unit_name,
creative_id,
creative_name,
creative_title,
channel_put,
report_date,
t1.pdate,
t1.psrc 
;


