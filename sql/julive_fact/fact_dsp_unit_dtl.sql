-- dsp_unit(dsp-单元信息表)清洗SQL 

set etl_date = date_add(current_date(),-1);

set hive.execution.engine=spark;
set spark.app.name=fact_dsp_unit_dtl;
set mapred.job.name=fact_dsp_unit_dtl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;


insert overwrite table julive_fact.fact_dsp_unit_dtl partition(pdate)

select 

t1.account_id, -- 账户id
t1.account, -- 账户
t1.plan_id, -- 计划ID
t1.plan_name, -- 计划名
t1.unit_id, -- 单元id
t1.unit_name, -- 单元名
t1.media_type, -- 媒体类型
t1.product_type, -- 媒体形态 
t1.max_price, -- 最高出价
t1.pause, -- 是否暂停
t1.unit_status, -- 状态 31-有效 32-暂停推广 33-推广计划暂停推广
t1.price_ratio, -- 无线出价比例
t1.pc_price_ratio, -- 计算机出价比例
t1.accu_price_factor, -- 精确出价比例
t1.wide_price_factor, -- 广泛出价比例
t1.word_price_factor, -- 短语出价比例
t1.segment_recommend_status, -- 图片素材配图开关 1 – 关闭 0 – 开启，默认开启
t1.match_price_status, -- 分匹配状态 0 开启，要求精确系数>= 短语系数>=广泛系数，且三个比例系数均不能为空 1关
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

from ods.dsp_unit t1 
where pdate = regexp_replace(${hiveconf:etl_date},'-','')
group by 
t1.account,
t1.media_type,
t1.product_type,
t1.account_id,
t1.plan_id,
t1.plan_name,
t1.unit_id,
t1.unit_name,
t1.max_price,
t1.pause,
t1.negative_words,
t1.exact_negative_words,
t1.unit_status,
t1.price_ratio,
t1.pc_price_ratio,
t1.accu_price_factor,
t1.wide_price_factor,
t1.word_price_factor,
t1.segment_recommend_status,
t1.match_price_status,
t1.snapshot_date
,t1.pdate
;
