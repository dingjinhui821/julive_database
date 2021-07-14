-- dsp_plan(dsp-计划信息表)清洗SQL 

set etl_date = date_add(current_date(),-1);

set hive.execution.engine=spark;
set spark.app.name=fact_dsp_plan_dtl;
set mapred.job.name=fact_dsp_plan_dtl;
set spark.yarn.queue=etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

insert overwrite table julive_fact.fact_dsp_plan_dtl partition(pdate)

select 

t1.account_id, -- 账户id
t1.account, -- 账户
t1.plan_id, -- 计划ID
t1.plan_name, -- 计划名
t1.plan_type, -- 计划类型 
t1.media_type, -- 媒体类型
t1.product_type, -- 媒体形态
t1.budget, -- 预算 
t1.device, -- 推广设备
t1.region_target, -- 推广区域
t1.region_target_code, -- 推广区域码 
t1.pause, -- 是否暂停
t1.price_ratio, -- 无线出价比例
t1.pc_price_ratio, -- 计算机出价比例
t1.bid_prefer, -- 出价优先 1：计算机优先 2：移动优先
t1.plan_status, -- 推广计划状态 21-有效 22-处于暂停时段 23-暂停推广 24-推广计划预算不足 25-账户预算不足
t1.show_prob, -- 创意展现方式 1 - 优选 2 - 轮显
get_json_object(t1.negative_words,'$') as negative_words, -- 否定关键词
size(split(substr(get_json_object(t1.negative_words,'$'),2,length(get_json_object(t1.negative_words,'$')) - 2),",")) as negative_words_num, -- 否定关键词个数
get_json_object(t1.exact_negative_words,'$') as exact_negative_words, -- 精确否定关键词
size(split(substr(get_json_object(t1.exact_negative_words,'$'),2,length(get_json_object(t1.exact_negative_words,'$')) - 2),",")) as exact_negative_words_num, -- 否定关键词个数
from_unixtime(max(t1.create_datetime),"yyyy-MM-dd HH:mm:ss") as create_datetime,
from_unixtime(max(t1.update_datetime),"yyyy-MM-dd HH:mm:ss") as update_datetime,
from_unixtime(t1.snapshot_date,"yyyy-MM-dd") as report_date,
current_timestamp() as etl_time,
-- regexp_replace(from_unixtime(t1.snapshot_date,"yyyy-MM-dd"),'-','') as pdate -- 全量刷新数据用 
t1.pdate 

from ods.dsp_plan t1 
where pdate = regexp_replace(${hiveconf:etl_date},'-','')
group by 
t1.account,
t1.media_type,
t1.product_type,
t1.account_id,
t1.plan_id,
t1.plan_name,
t1.budget,
t1.plan_type,
t1.device,
t1.negative_words,
t1.exact_negative_words,
t1.region_target,
t1.region_target_code,
t1.pause,
t1.price_ratio,
t1.pc_price_ratio,
t1.bid_prefer,
t1.plan_status,
t1.show_prob,
t1.snapshot_date
,t1.pdate 
;

