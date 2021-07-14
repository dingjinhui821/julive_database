set etl_date = '${hiveconf:etlDate}';
set etl_yestoday = '${hiveconf:etlYestoday}';

--set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 测试专用 
--set etl_yestoday = regexp_replace(date_add(current_date(),-2),'-',''); -- 测试专用 

set hive.execution.engine=spark; 
set spark.app.name=fact_dsp_sem_unit_dtl;
set mapred.job.name=fact_dsp_sem_unit_dtl; 
set spark.yarn.queue=etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;


-- 百度单元数据 
drop table if exists tmp_dev_1.tmp_dsp_sem_unit;
create table tmp_dev_1.tmp_dsp_sem_unit as 

select 

tmp.account_id,
tmp.account_name,
tmp.media_id,
case                                                  
when tmp.media_id = 1 then '百度'
when tmp.media_id = 2 then '360' 
when tmp.media_id = 3 then '搜狗'
when tmp.media_id = 4 then '今日头条'
when tmp.media_id = 5 then '腾讯智汇推'
when tmp.media_id = 6 then '百度信息流' 
when tmp.media_id = 7 then 'APP'
when tmp.media_id = 8 then '其他'
when tmp.media_id = 9 then '免费'
when tmp.media_id = 10 then '导航'
when tmp.media_id = 11 then '神马'
when tmp.media_id = 12 then '厂商'
when tmp.media_id = 13 then '微信'
when tmp.media_id = 14 then '端口'
else "其他" end                               as media_name,
tmp.module_id,
case 
when tmp.module_id = 1 then 'FEED'
when tmp.module_id = 3 then 'APP'
when tmp.module_id = 4 then 'SEM'
when tmp.module_id = 0 then '全部'
else "其他" end                               as module_name,
tmp.plan_id,
tmp.plan_name,
tmp.unit_id,
tmp.unit_name,
tmp.max_price,
tmp.pause,
tmp.negative_words,
size(split(tmp.negative_words,',')) as negative_words_num, 
tmp.exact_negative_words,
size(split(tmp.exact_negative_words,',')) as exact_negative_words_num, 
tmp.unit_status,
tmp.price_ratio,
tmp.pc_price_ratio,
tmp.accu_price_factor,
tmp.wide_price_factor,
tmp.word_price_factor,
tmp.segment_recommend_status,
tmp.match_price_status,
tmp.create_date,
current_timestamp() as etl_time,

${hiveconf:etl_date} as pdate 

from ( 
select  

t.*,
row_number()over(partition by t.account_id,t.media_id,t.unit_id order by t.create_date desc) as rn 

from ( -- 包含截至昨天的全量最新数据  
select -- 计算ods最新增量数据 baidu 

t1.account_id                          as account_id,
t1.account                             as account_name,
t1.media_type                          as media_id,
t1.product_type                        as module_id,
t1.plan_id                             as plan_id,
t1.plan_name                           as plan_name,
t1.unit_id                             as unit_id,
t1.unit_name                           as unit_name,

t1.max_price,
t1.pause,

regexp_replace((regexp_replace((regexp_replace(get_json_object(t1.negative_words,'$'),'"','')),'\\[','')),'\\]','')       as negative_words, -- 否定关键词
regexp_replace((regexp_replace((regexp_replace(get_json_object(t1.exact_negative_words,'$'),'"','')),'\\[','')),'\\]','') as exact_negative_words,

t1.unit_status,
t1.price_ratio,
t1.pc_price_ratio,
t1.accu_price_factor,
t1.wide_price_factor,
t1.word_price_factor,
t1.segment_recommend_status,
t1.match_price_status,
regexp_replace(from_unixtime(t1.snapshot_date,"yyyy-MM-dd"),'-','') as create_date 

from ods.dsp_unit t1 
where t1.pdate = ${hiveconf:etl_date} -- 获取etl日期 

union all 

select -- 计算ods最新增量数据 360 

t1.account_id                          as account_id,
t1.account                             as account_name,
t1.media_type                          as media_id,
t1.product_type                        as module_id,
t1.plan_id                             as plan_id,
t1.plan_name                           as plan_name,
t1.unit_id                             as unit_id,
t1.unit_name                           as unit_name,

t1.max_price,
t1.pause,

regexp_replace((regexp_replace((regexp_replace(get_json_object(t1.negative_words,'$'),'"','')),'\\[','')),'\\]','')       as negative_words, -- 否定关键词
regexp_replace((regexp_replace((regexp_replace(get_json_object(t1.exact_negative_words,'$'),'"','')),'\\[','')),'\\]','') as exact_negative_words,

t1.unit_status,
t1.price_ratio,
t1.pc_price_ratio,
t1.accu_price_factor,
t1.wide_price_factor,
t1.word_price_factor,
t1.segment_recommend_status,
t1.match_price_status,
regexp_replace(from_unixtime(t1.snapshot_date,"yyyy-MM-dd"),'-','') as create_date 

from ods.dsp_qihoo_unit t1 
where regexp_replace(from_unixtime(t1.snapshot_date,"yyyy-MM-dd"),'-','') = ${hiveconf:etl_date} -- 获取etl日期 

union all 

select -- 计算ods最新增量数据 神马 

t1.account_id                          as account_id,
t1.account                             as account_name,
t1.media_type                          as media_id,
t1.product_type                        as module_id,
t1.plan_id                             as plan_id,
t1.plan_name                           as plan_name,
t1.unit_id                             as unit_id,
t1.unit_name                           as unit_name,

t1.max_price,
t1.pause,

regexp_replace((regexp_replace((regexp_replace(get_json_object(t1.negative_words,'$'),'"','')),'\\[','')),'\\]','')       as negative_words, -- 否定关键词
regexp_replace((regexp_replace((regexp_replace(get_json_object(t1.exact_negative_words,'$'),'"','')),'\\[','')),'\\]','') as exact_negative_words,

t1.unit_status,
t1.price_ratio,
t1.pc_price_ratio,
t1.accu_price_factor,
t1.wide_price_factor,
t1.word_price_factor,
t1.segment_recommend_status,
t1.match_price_status,
regexp_replace(from_unixtime(t1.snapshot_date,"yyyy-MM-dd"),'-','') as create_date 

from ods.dsp_shenma_unit t1 
where regexp_replace(from_unixtime(t1.snapshot_date,"yyyy-MM-dd"),'-','') = ${hiveconf:etl_date} -- 获取etl日期 

union all  

select -- 计算ods最新增量数据 搜狗

t1.account_id                          as account_id,
t1.account                             as account_name,
t1.media_type                          as media_id,
t1.product_type                        as module_id,
t1.plan_id                             as plan_id,
t1.plan_name                           as plan_name,
t1.unit_id                             as unit_id,
t1.unit_name                           as unit_name,

t1.max_price,
t1.pause,

regexp_replace((regexp_replace((regexp_replace(get_json_object(t1.negative_words,'$'),'"','')),'\\[','')),'\\]','')       as negative_words, -- 否定关键词
regexp_replace((regexp_replace((regexp_replace(get_json_object(t1.exact_negative_words,'$'),'"','')),'\\[','')),'\\]','') as exact_negative_words,

t1.unit_status,
t1.price_ratio,
t1.pc_price_ratio,
t1.accu_price_factor,
t1.wide_price_factor,
t1.word_price_factor,
t1.segment_recommend_status,
t1.match_price_status,
regexp_replace(from_unixtime(t1.snapshot_date,"yyyy-MM-dd"),'-','') as create_date 

from ods.dsp_sogou_unit t1 
where regexp_replace(from_unixtime(t1.snapshot_date,"yyyy-MM-dd"),'-','') = ${hiveconf:etl_date} -- 获取etl日期 
union all 

select -- 获取前天全量快照数据 

t1.account_id,
t1.account_name,
t1.media_id,
t1.module_id,
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
t1.create_date

from julive_fact.fact_dsp_sem_unit_dtl t1 -- hive版本存在bug 
where t1.pdate = ${hiveconf:etl_yestoday} -- 获取etl日期的前一天日期 
) t 
) tmp 
where tmp.rn = 1 -- 获取昨天最新数据 
;



insert overwrite table julive_fact.fact_dsp_sem_unit_dtl partition(pdate) 
select 

account_id,
account_name,
media_id,
media_name,
module_id,
module_name,
plan_id,
plan_name,
unit_id,
unit_name,
-- 计算 
max_price,
pause,
negative_words,
negative_words_num,
exact_negative_words,
exact_negative_words_num,
unit_status,
price_ratio,
pc_price_ratio,
accu_price_factor,
wide_price_factor,
word_price_factor,
segment_recommend_status,
match_price_status,

create_date,
etl_time,
pdate 

from tmp_dev_1.tmp_dsp_sem_unit
;
    
