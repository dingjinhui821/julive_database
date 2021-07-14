-- 市场关键词主题表 
-- julive_topic.topic_sem_keyword_day_indi  
-- tmp_dev_1.tmp_market_dsp_sem_keyword_day_indi : 2019-09-19 

-- 传参,补数逻辑 
set etl_date = '${hiveconf:etlDate}'; -- 取昨天日期 
set etl_yestoday = '${hiveconf:etlYestoday}'; -- 取前天日期 

------------------------------------------------------------------------------------------------------
-- 第一步：计算基础表
-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 取昨天日期 
-- set etl_yestoday = regexp_replace(date_add(current_date(),-2),'-',''); -- 取昨天日期 

set spark.app.name=tmp_market_dsp_sem_keyword_day_indi;
set hive.execution.engine=spark;
set mapreduce.job.queuename=root.etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

set spark.yarn.queue=etl;
set spark.executor.cores=1;
set spark.executor.memory=16g;
set spark.executor.instances=14;
set spark.yarn.executor.memoryOverhead=8192;

set hive.prewarm.enabled=true;
set hive.prewarm.numcontainers=14;

set hive.optimize.reducededuplication.min.reducer=4;
set hive.optimize.reducededuplication=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;
set hive.merge.smallfiles.avgsize=16000000;
set hive.merge.size.per.task=256000000;
set hive.merge.sparkfiles=true;
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=209715200;
set hive.optimize.bucketmapjoin.sortedmerge=false;
set hive.map.aggr.hash.percentmemory=0.5;
set hive.map.aggr=true;
set hive.optimize.sort.dynamic.partition=false;
set hive.stats.autogather=true;
set hive.stats.fetch.column.stats=true;
set hive.compute.query.using.stats=true;
set hive.limit.pushdown.memory.usage=0.4;
set hive.optimize.index.filter=true;
set hive.exec.reducers.bytes.per.reducer=67108864;
set hive.smbjoin.cache.rows=10000;
set hive.fetch.task.conversion=more;
set hive.fetch.task.conversion.threshold=1073741824;
set hive.optimize.ppd=true;
set hive.mapjoin.localtask.max.memory.usage=0.999;

-- ---------------------------------------------------------------------------------------------------------------------
-- 1 计算线下指标 ：channel_id,channel_put,日期；channel_id,keyword_id,日期；| 取每个关键词首次匹配的order_id 
drop table if exists tmp_dev_1.tmp_market_offline_indi;
create table tmp_dev_1.tmp_market_offline_indi as 

select 

min(t4.city_id)                                                                                          as city_id,
max(t4.city_name)                                                                                        as city_name, 
t1.channel_id                                                                                            as channel_id,
max(t4.channel_name)                                                                                     as channel_name,
null                                                                                                     as account_id,
null                                                                                                     as account_name,
max(t4.media_id)                                                                                         as media_id,
max(t4.media_name)                                                                                       as media_name,
max(t4.module_id)                                                                                        as module_id,
max(t4.module_name)                                                                                      as module_name,

null                                                                                                     as plan_id,
if(max(if(t1.keyword_id != -1 or t1.keyword_id is not null,1,0))=0,split(t1.channel_put2,"\\|")[0],null) as plan_name,
null                                                                                                     as unit_id,
if(max(if(t1.keyword_id != -1 or t1.keyword_id is not null,1,0))=0,split(t1.channel_put2,"\\|")[1],null) as unit_name,
if(max(if(t1.keyword_id != -1 or t1.keyword_id is not null,1,0))=0,null,max(t1.keyword_id))              as keyword_id,
if(max(if(t1.keyword_id != -1 or t1.keyword_id is not null,1,0))=0,split(t1.channel_put2,"\\|")[2],null) as keyword_name,

max(if(t1.keyword_id != -1 and t1.keyword_id is not null,1,0))                                           as has_keyword_id,
max(t1.channel_put)                                                                                      as channel_put,
t1.channel_put2                                                                                          as channel_put2,
t1.clue_create_date                                                                                      as pdate,
-- 关键词粒度线下指标  
sum(t1.leave_phone_num)                                                                                  as leave_phone_num, -- 留电量 
count(t1.clue_id)                                                                                        as clue_num, -- 线索量 
count(if(t1.is_distribute=1,t1.clue_id,null))                                                            as distribute_num, -- 上户量 
sum(t2.see_num)                                                                                          as see_num, -- 带看量 
sum(t3.subscribe_num)                                                                                    as subscribe_num, -- 认购量 
sum(t5.sign_num)                                                                                         as sign_num,
-- first关键词粒度线下指标  
count(if(t1.is_first_website = 1,t1.clue_id,null))                                                       as first_clue_num, -- 首次线索量 
count(if(t1.is_first_website = 1 and t1.is_distribute = 1,t1.clue_id,null))                              as first_distribute_num, -- 首次上户量 
sum(if(t1.is_first_website = 1,t2.see_num,null))                                                         as first_see_num, -- 首次带看量 
sum(if(t1.is_first_website = 1,t3.subscribe_num,null))                                                   as first_subscribe_num, -- 首次认购量 
sum(if(t1.is_first_website = 1,t5.sign_num,null))                                                        as first_sign_num -- 首次签约量 

from (

select 

tmp.channel_id,
if(tmp.keyword_id = -1 or tmp.keyword_id is null,tmp.channel_put,tmp.keyword_id) as channel_put2, -- 有关键词id 用关键词id ，否则用channel_put 
tmp.keyword_id as keyword_id,
tmp.channel_put as channel_put,
tmp.clue_id,
tmp.is_distribute,
tmp.clue_create_date,
tmp.is_first_website,
tmp.leave_phone_num

from (

select 

t.channel_id,
t.clue_id,
t.is_distribute,
regexp_replace(to_date(t.clue_create_time),'-','') as clue_create_date,
t.channel_put,

max(t.keyword_id) as keyword_id,
min(t.is_first_website) as is_first_website,

count(1) as leave_phone_num

from julive_fact.fact_site_behavior_dtl t 

where to_date(t.create_time) <= to_date(t.clue_create_time) -- 剔除未来留电情况 
and t.from_source = 1 -- website_op 
and t.channel_put is not null 
and t.channel_put != '' 

group by -- 每天 每个渠道 每个关键词 每个订单1行 
t.channel_id,
t.channel_put,
t.clue_id,
t.is_distribute,
regexp_replace(to_date(t.clue_create_time),'-','')

) tmp

) t1 left join ( -- 上户量 

select

order_id,
count(id) as see_num

from ods.yw_see_project
where status >= 40
and status < 60
group by order_id

) t2 on t1.clue_id = t2.order_id 
left join ( -- 认购量 : 含退-含外联 

select 

order_id,
count(id) subscribe_num

from ods.yw_subscribe 
where subscribe_type in (1,4) 
and status in (1,2)
group by order_id

) t3 on t1.clue_id = t3.order_id 
left join ( -- 签约量 : 含退-含外联 

select 

order_id,
count(id) sign_num

from ods.yw_sign  
where sign_type in (1,4) 
and status in (1,2)
group by order_id

) t5 on t1.clue_id = t5.order_id 
join julive_dim.dim_channel_info t4 on t1.channel_id = t4.channel_id 

group by 
t1.channel_id,
t1.channel_put2,
t1.clue_create_date 
;


-- 2 计算关键词历史数据基础信息,用于[反匹配]线下产生的线索关联不到线上关键词数据情况下的关键词基础信息 
insert overwrite table tmp_dev_1.tmp_before_keyword_base_info partition(pdate) 

select 

tmp.city_id,
tmp.city_name,
tmp.channel_id,
tmp.channel_name,
tmp.media_id,
tmp.media_name,
tmp.module_id,
tmp.module_name,
tmp.account_id,
tmp.account_name,
tmp.plan_id,
tmp.plan_name,
tmp.unit_id,
tmp.unit_name,
tmp.keyword_id,
tmp.keyword_name,
tmp.channel_put,
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate 

from (
select  

t.city_id,
t.city_name,
t.channel_id,
t.channel_name,
t.media_id,
t.media_name,
t.module_id,
t.module_name,
t.account_id,
t.account_name,
t.plan_id,
t.plan_name,
t.unit_id,
t.unit_name,
t.keyword_id,
t.keyword_name,
t.channel_put,
row_number()over(partition by t.channel_id,t.channel_put order by t.pdate desc) as rn 

from (
select 

t.city_id,
t.city_name,
t.channel_id,
t.channel_name,
t.media_id,
t.media_name,
t.module_id,
t.module_name,
t.account_id,
t.account_name,
t.plan_id,
t.plan_name,
t.unit_id,
t.unit_name,
t.keyword_id,
t.keyword_name,
t.channel_put,
t.pdate 

from julive_fact.fact_dsp_sem_keyword_day_indi t 
where t.pdate = ${hiveconf:etl_date} -- 昨天 
and channel_id is not null 
and channel_put is not null 
and channel_put != '' 

union all 

select 

t.city_id,
t.city_name,
t.channel_id,
t.channel_name,
t.media_id,
t.media_name,
t.module_id,
t.module_name,
t.account_id,
t.account_name,
t.plan_id,
t.plan_name,
t.unit_id,
t.unit_name,
t.keyword_id,
t.keyword_name,
t.channel_put,
t.pdate 

from tmp_dev_1.tmp_before_keyword_base_info t 
where t.pdate = ${hiveconf:etl_yestoday} -- 前天 
and channel_id is not null 
and channel_put is not null 
and channel_put != '' 

) t 

) tmp 
where tmp.rn = 1 
;


-- 3 为线下数据冗余 关键词 维度属性 
drop table if exists tmp_dev_1.tmp_market_offline_add_kw_indi;
create table tmp_dev_1.tmp_market_offline_add_kw_indi as 

select 

if(t1.city_id is null,t2.city_id,t1.city_id)                                        as city_id,
if(t1.city_name is null or t1.city_name = '',t2.city_name,t1.city_name)             as city_name,
if(t1.channel_id is null,t2.channel_id,t1.channel_id)                               as channel_id,
if(t1.channel_name is null or t1.channel_name = '',t2.channel_name,t1.channel_name) as channel_name,

coalesce(t2.account_id,t3.account_id)                                               as account_id,
coalesce(t2.account_name,t3.account_name)                                           as account_name,
coalesce(t1.media_id,t2.media_id,t3.media_id)                                       as media_id,
coalesce(t1.media_name,t2.media_name,t3.media_name)                                 as media_name,
coalesce(t1.module_id,t2.module_id,t3.module_id)                                    as module_id,
coalesce(t1.module_name,t2.module_name,t3.module_name)                              as module_name,

coalesce(t2.plan_id,t3.plan_id)                                                     as plan_id,
coalesce(t1.plan_name,split(t1.channel_put,"\\|")[0],t2.plan_name,t3.plan_name)     as plan_name,
coalesce(t2.unit_id,t3.unit_id)                                                     as unit_id,
coalesce(t1.unit_name,split(t1.channel_put,"\\|")[1],t2.unit_name,t3.unit_name)     as unit_name,
coalesce(t2.keyword_id,t3.keyword_id,t1.keyword_id)                                 as keyword_id,
coalesce(t2.keyword_name,split(t1.channel_put,"\\|")[2],t3.keyword_name,t1.keyword_name) as keyword_name,

t1.has_keyword_id,
t1.channel_put,
t1.channel_put2,
t1.pdate,

t1.leave_phone_num,
t1.clue_num,
t1.distribute_num,
t1.see_num,
t1.subscribe_num,
t1.sign_num,
t1.first_clue_num,
t1.first_distribute_num,
t1.first_see_num,
t1.first_subscribe_num,
t1.first_sign_num  

from (

select * 
from tmp_dev_1.tmp_market_offline_indi 
where has_keyword_id = 1 

) t1 left join (

select tmp.* 
from (
select t.*,
row_number()over(partition by t.channel_id,t.keyword_id order by t.pdate desc) as rn 
from tmp_dev_1.tmp_before_keyword_base_info t 
where t.pdate = ${hiveconf:etl_date} 
) tmp 
where tmp.rn = 1 

) t2 on t1.channel_id = t2.channel_id and t1.channel_put2 = t2.keyword_id 

left join (

select 

account_id                                    as account_id,
account                                       as account_name,
media_type                                    as media_id,
case 
when media_type = 1 then '百度' 
when media_type = 2 then '360' 
when media_type = 3 then '搜狗' 
when media_type = 4 then '今日头条' 
when media_type = 5 then '腾讯智汇推' 
when media_type = 6 then '百度信息流' 
when media_type = 7 then 'APP' 
when media_type = 8 then '其他' 
when media_type = 9 then '免费' 
when media_type = 10 then '导航' 
when media_type = 11 then '神马' 
when media_type = 12 then '厂商' 
when media_type = 13 then '微信' 
when media_type = 14 then '端口' 
end                                           as media_name,
product_type                                  as module_id,
case 
when product_type = 1 then 'FEED'
when product_type = 3 then 'APP'
when product_type = 4 then 'SEM'
when product_type = 0 then '全部'
end                                           as module_name,
plan_id                                       as plan_id,
plan_name                                     as plan_name,
unit_id                                       as unit_id,
unit_name                                     as unit_name,
word_id                                       as keyword_id,
word                                          as keyword_name

from ods.dsp_account_structure
where structure_type = 4 

) t3 on t1.channel_put2 = t3.keyword_id 
;


insert into table tmp_dev_1.tmp_market_offline_add_kw_indi 

select 

if(t1.city_id is null,t2.city_id,t1.city_id)                                        as city_id,
if(t1.city_name is null or t1.city_name = '',t2.city_name,t1.city_name)             as city_name,
if(t1.channel_id is null,t2.channel_id,t1.channel_id)                               as channel_id,
if(t1.channel_name is null or t1.channel_name = '',t2.channel_name,t1.channel_name) as channel_name,
t2.account_id                                                                       as account_id,
t2.account_name                                                                     as account_name,
coalesce(t1.media_id,t2.media_id)                                                   as media_id,
coalesce(t1.media_name,t2.media_name)                                               as media_name,
coalesce(t1.module_id,t2.module_id)                                                 as module_id,
coalesce(t1.module_name,t2.module_name)                                             as module_name,

t2.plan_id                                                                          as plan_id,
coalesce(t1.plan_name,split(t1.channel_put,"\\|")[0],t2.plan_name)                  as plan_name,
t2.unit_id                                                                          as unit_id,
coalesce(t1.unit_name,split(t1.channel_put,"\\|")[1],t2.unit_name)                  as unit_name,

t2.keyword_id                                                                       as keyword_id,
coalesce(t2.keyword_name,t1.keyword_name,split(t1.channel_put,"\\|")[2])            as keyword_name,

t1.has_keyword_id,
t1.channel_put,
t1.channel_put2,
t1.pdate,
t1.leave_phone_num,
t1.clue_num,
t1.distribute_num,
t1.see_num,
t1.subscribe_num,
t1.sign_num,
t1.first_clue_num,
t1.first_distribute_num,
t1.first_see_num,
t1.first_subscribe_num,
t1.first_sign_num 

from (

select * 
from tmp_dev_1.tmp_market_offline_indi 
where has_keyword_id = 0  

) t1 left join (

select tmp.* 
from (
select t.*,
row_number()over(partition by t.channel_id,t.channel_put order by t.pdate desc) as rn 
from tmp_dev_1.tmp_before_keyword_base_info t 
where t.pdate = ${hiveconf:etl_date} 
) tmp 
where tmp.rn = 1 

) t2 on t1.channel_id = t2.channel_id 
    and t1.channel_put2 = t2.channel_put 
;

-- 4 计算跳出量指标 
insert overwrite table tmp_dev_1.tmp_market_userlogs_info partition(pdate) 

select 

t.city_name,
t.channel_id,
t.channel_put,
sum(t.jump_num) as jump_num,
sum(t.uv) as uv,
t.pdate

from dm.dm_jump_rate t -- 埋点 跳出率
where t.pdate = ${hiveconf:etl_date} -- 取数日期 
and t.platform = 'baidu' 

group by 
t.city_name,
t.channel_id,
t.channel_put,
t.pdate 
;

-- ------------------------------------------------------------------------------------------------------------------
-- 5 计算指标结果表 :join外部数据及业务数据逻辑 
insert overwrite table tmp_dev_1.tmp_market_dsp_sem_keyword_day_indi partition(pdate,pclue) 

select 

coalesce(t1.city_id,t2.city_id,t3.city_id)                                              as city_id, -- 城市 
coalesce(t1.city_name,t2.city_name,t3.city_name)                                        as city_name, 

coalesce(t1.media_id,t2.media_id,t3.media_id)                                           as media_id, -- 媒体 
coalesce(t1.media_name,t2.media_name,t3.media_name)                                     as media_name, -- 媒体名称 

coalesce(t1.module_id,t2.module_id,t3.module_id)                                        as module_id, -- 产品形态 
coalesce(t1.module_name,t2.module_name,t3.module_name)                                  as module_name,

coalesce(t1.channel_id,t2.channel_id,t3.channel_id)                                     as channel_id, -- 渠道 
coalesce(t1.channel_name,t2.channel_name,t3.channel_name)                               as channel_name,

coalesce(t1.account_id,t2.account_id,t3.account_id)                                     as account_id, -- 账户 
coalesce(t1.account_name,t2.account_name,t3.account_name)                               as account_name,

coalesce(t1.plan_id,t2.plan_id,t3.plan_id)                                              as plan_id, -- 计划 
coalesce(t1.plan_name,t2.plan_name,t3.plan_name)                                        as plan_name,

coalesce(t1.unit_id,t2.unit_id,t3.unit_id)                                              as unit_id, -- 单元 
coalesce(t1.unit_name,t2.unit_name,t3.unit_name)                                        as unit_name,

coalesce(t1.keyword_id,t2.keyword_id,t3.keyword_id)                                     as keyword_id, -- 关键词 
coalesce(t1.keyword_name,t2.keyword_name,t3.keyword_name)                               as keyword_name,

t1.match_type                                                                           as match_type,

t1.pc_average_ranking                                                                   as pc_average_ranking, -- 20191214添加
t1.m_average_ranking                                                                    as m_average_ranking, -- 20191214添加
t1.pc_url                                                                               as pc_url, -- 20191214添加
t1.m_url                                                                                as m_url, -- 20191214添加

t1.show_num                                                                             as show_num, -- 展 
t1.click_num                                                                            as click_num, -- 点 
t1.bill_cost                                                                            as bill_cost, -- 20191214添加
t1.cost                                                                                 as cost, -- 消 

-- 每个keyword 
coalesce(t2.leave_phone_num,t3.leave_phone_num)                                         as leave_phone_num, -- 留电量 
coalesce(t2.clue_num,t3.clue_num)                                                       as clue_num, -- 线索量 
coalesce(t2.distribute_num,t3.distribute_num)                                           as distribute_num, -- 上户量 
coalesce(t2.see_num,t3.see_num)                                                         as see_num, -- 带看量
coalesce(t2.subscribe_num,t3.subscribe_num)                                             as subscribe_num, -- 认购量 
coalesce(t2.sign_num,t3.sign_num)                                                       as sign_num,
null                                                                                    as returned_money,

-- 首个keyword 
coalesce(t2.first_clue_num,t3.first_clue_num)                                           as first_clue_num,
coalesce(t2.first_distribute_num,t3.first_distribute_num)                               as first_distribute_num,
coalesce(t2.first_see_num,t3.first_see_num)                                             as first_see_num,
coalesce(t2.first_subscribe_num,t3.first_subscribe_num)                                 as first_subscribe_num,
coalesce(t2.first_sign_num,t3.first_sign_num)                                           as first_sign_num,
null                                                                                    as first_returned_money,

t4.jump_num                                                                             as jump_num, -- 跳出量 
t4.uv                                                                                   as uv, -- UV 

current_timestamp()                                                                     as etl_time,
coalesce(t1.pdate,t2.pdate,t3.pdate)                                                    as pdate,
if(t2.clue_num is not null or t3.clue_num is not null,'hasclue','nohasclue')            as pclue

-- -------------------------------------------------------------------------------------------------------------------------------------
from ( -- 取关键词etl日期数据 

select * 
from julive_fact.fact_dsp_sem_keyword_day_indi 
where pdate = ${hiveconf:etl_date} 

) t1 full join ( -- 通过keyword_id关联的逻辑 

select * 
from tmp_dev_1.tmp_market_offline_add_kw_indi 
where pdate = ${hiveconf:etl_date}
and has_keyword_id = 1 -- 存在关键词 
and channel_put2 != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and channel_put2 != 'YD-通配-广州-1|低价|清远房价走势2019预测'

) t2 on t1.channel_id = t2.channel_id and t1.keyword_id = t2.channel_put2 and t1.pdate = t2.pdate 
full join (

select * 
from tmp_dev_1.tmp_market_offline_add_kw_indi 
where pdate = ${hiveconf:etl_date}
and has_keyword_id = 0 -- 不存在关键词留电 
and channel_put2 != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and channel_put2 != 'YD-通配-广州-1|低价|清远房价走势2019预测'

) t3 on t1.channel_id = t3.channel_id and t1.channel_put = t3.channel_put2 and t1.pdate = t3.pdate 

left join tmp_dev_1.tmp_market_userlogs_info t4 on t1.city_name = t4.city_name and t1.channel_id = t4.channel_id and t1.channel_put = t4.channel_put and t1.pdate = t4.pdate 
;


-- -------------------------------------------------------------------------------------------------
-- 6 备份产生指标数据  
insert overwrite table tmp_dev_1.tmp_sem_keyword_day_indi partition(pdate) 

select 

city_id,
city_name,
media_id,
media_name,
module_id,
module_name,
channel_id,
channel_name,
account_id,
account_name,
plan_id,
plan_name,
unit_id,
unit_name,
keyword_id,
keyword_name,
match_type,

pc_average_ranking, -- 20191214添加 
m_average_ranking, -- 20191214添加 
pc_url, -- 20191214添加 
m_url, -- 20191214添加 

show_num,
click_num,
bill_cost, -- 20191214添加 
cost,

-- 每一个关键词 
leave_phone_num,
clue_num,
distribute_num,
see_num,
subscribe_num,
sign_num,
returned_money,

-- 首个关键词 
first_clue_num,
first_distribute_num,
first_see_num,
first_subscribe_num,
first_sign_num,
first_returned_money,

jump_num,
uv,

etl_time,
pdate 

from tmp_dev_1.tmp_market_dsp_sem_keyword_day_indi
where pdate = ${hiveconf:etl_date} 
and pclue = 'hasclue'
;


-- 7 刷新产生线索历史数据 
insert overwrite table tmp_dev_1.tmp_market_dsp_sem_keyword_day_indi partition(pdate,pclue) 

select 

coalesce(t1.city_id,t2.city_id,t3.city_id)                                           as city_id, -- 城市 
coalesce(t1.city_name,t2.city_name,t3.city_name)                                     as city_name, 
coalesce(t1.media_id,t2.media_id,t3.media_id)                                        as media_id, -- 媒体 
coalesce(t1.media_name,t2.media_name,t3.media_name)                                  as media_name, -- 媒体名称 
coalesce(t1.module_id,t2.module_id,t3.module_id)                                     as module_id, -- 产品形态 
coalesce(t1.module_name,t2.module_name,t3.module_name)                               as module_name,
coalesce(t1.channel_id,t2.channel_id,t3.channel_id)                                  as channel_id, -- 渠道 
coalesce(t1.channel_name,t2.channel_name,t3.channel_name)                            as channel_name,
coalesce(t1.account_id,t2.account_id,t3.account_id)                                  as account_id, -- 账户 
coalesce(t1.account_name,t2.account_name,t3.account_name)                            as account_name,
coalesce(t1.plan_id,t2.plan_id,t3.plan_id)                                           as plan_id, -- 计划 
coalesce(t1.plan_name,t2.plan_name,t3.plan_name)                                     as plan_name,
coalesce(t1.unit_id,t2.unit_id,t3.unit_id)                                           as unit_id, -- 单元 
coalesce(t1.unit_name,t2.unit_name,t3.unit_name)                                     as unit_name,
coalesce(t1.keyword_id,t2.keyword_id,t3.keyword_id)                                  as keyword_id, -- 关键词 
coalesce(t1.keyword_name,t2.keyword_name,t3.keyword_name)                            as keyword_name,
t1.match_type                                                                        as match_type,

t1.pc_average_ranking                                                                as pc_average_ranking, -- 20191214添加
t1.m_average_ranking                                                                 as m_average_ranking, -- 20191214添加
t1.pc_url                                                                            as pc_url, -- 20191214添加
t1.m_url                                                                             as m_url, -- 20191214添加

t1.show_num                                                                          as show_num, -- 展 
t1.click_num                                                                         as click_num, -- 点 
t1.bill_cost                                                                         as bill_cost, -- 20191214添加
t1.cost                                                                              as cost, -- 消 
t1.leave_phone_num                                                                   as leave_phone_num, -- 留电量 

coalesce(t1.clue_num,t2.clue_num,t3.clue_num)                                        as clue_num, -- 线索量 
coalesce(t2.distribute_num,t3.distribute_num,t1.distribute_num)                      as distribute_num, -- 上户量 
coalesce(t2.see_num,t3.see_num,t1.see_num)                                           as see_num, -- 带看量 
coalesce(t2.subscribe_num,t3.subscribe_num,t1.subscribe_num)                         as subscribe_num, -- 认购量 
coalesce(t2.sign_num,t3.sign_num,t1.sign_num)                                        as sign_num, -- 签约量 
null                                                                                 as returned_money, -- 回款 

coalesce(t1.first_clue_num,t2.first_clue_num,t3.first_clue_num)                      as first_clue_num, -- 首次线索量 
coalesce(t2.first_distribute_num,t3.first_distribute_num,t1.first_distribute_num)    as first_distribute_num, -- 首次上户量 
coalesce(t2.first_see_num,t3.first_see_num,t1.first_see_num)                         as first_see_num, -- 首次带看量 
coalesce(t2.first_subscribe_num,t3.first_subscribe_num,t1.first_subscribe_num)       as first_subscribe_num, -- 首次认购量 
coalesce(t2.first_sign_num,t3.first_sign_num,t1.first_sign_num)                      as first_sign_num, -- 首次签约量 
null                                                                                 as first_returned_money, -- 首次回款

t1.jump_num                                                                          as jump_num, -- 跳出量 
t1.uv                                                                                as uv, -- UV 

current_timestamp()                                                                  as etl_time,
t1.pdate                                                                             as pdate,
'hasclue'                                                                            as pclue 

from (

select *
from tmp_dev_1.tmp_sem_keyword_day_indi 
where pdate >= '20190702'

) t1 left join (

select * 
from tmp_dev_1.tmp_market_offline_add_kw_indi  
where has_keyword_id = 1 -- 存在关键词 
and channel_put2 != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and channel_put2 != 'YD-通配-广州-1|低价|清远房价走势2019预测'

) t2 on t1.channel_id = t2.channel_id and t1.keyword_id = t2.channel_put2 and t1.pdate = t2.pdate 

left join (

select * 
from tmp_dev_1.tmp_market_offline_add_kw_indi 
where has_keyword_id = 0 -- 不存在关键词 
and channel_put2 != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and channel_put2 != 'YD-通配-广州-1|低价|清远房价走势2019预测'

) t3 on t1.channel_id = t3.channel_id and concat_ws('|',t1.plan_name,t1.unit_name,t1.keyword_name) = t3.channel_put2 and t1.pdate = t3.pdate 
;


insert overwrite table tmp_dev_1.tmp_market_dsp_sem_keyword_day_indi partition(pdate,pclue) 

select 

coalesce(t1.city_id,t2.city_id)                              as city_id, -- 城市 
coalesce(t1.city_name,t2.city_name)                          as city_name, 
coalesce(t1.media_id,t2.media_id)                            as media_id, -- 媒体 
coalesce(t1.media_name,t2.media_name)                        as media_name, -- 媒体名称 
coalesce(t1.module_id,t2.module_id)                          as module_id, -- 产品形态 
coalesce(t1.module_name,t2.module_name)                      as module_name,
coalesce(t1.channel_id,t2.channel_id)                        as channel_id, -- 渠道 
coalesce(t1.channel_name,t2.channel_name)                    as channel_name,
coalesce(t1.account_id,t2.account_id)                        as account_id, -- 账户 
coalesce(t1.account_name,t2.account_name)                    as account_name,
coalesce(t1.plan_id,t2.plan_id)                              as plan_id, -- 计划 
coalesce(t1.plan_name,t2.plan_name)                          as plan_name,
coalesce(t1.unit_id,t2.unit_id)                              as unit_id, -- 单元 
coalesce(t1.unit_name,t2.unit_name)                          as unit_name,
coalesce(t1.keyword_id,t2.keyword_id)                        as keyword_id, -- 关键词 
coalesce(t1.keyword_name,t2.keyword_name)                    as keyword_name,
t1.match_type                                                as match_type,

t1.pc_average_ranking                                        as pc_average_ranking, -- 20191214添加
t1.m_average_ranking                                         as m_average_ranking, -- 20191214添加
t1.pc_url                                                    as pc_url, -- 20191214添加
t1.m_url                                                     as m_url, -- 20191214添加

t1.show_num                                                  as show_num, -- 展 
t1.click_num                                                 as click_num, -- 点 
t1.bill_cost                                                 as bill_cost,--20191214添加
t1.cost                                                      as cost, -- 消 
t1.leave_phone_num                                           as leave_phone_num, -- 留电量 

coalesce(t1.clue_num,t2.clue_num)                            as clue_num, -- 线索量 
coalesce(t2.distribute_num,t1.distribute_num)                as distribute_num, -- 上户量 
coalesce(t2.see_num,t1.see_num)                              as see_num, -- 带看量 
coalesce(t2.subscribe_num,t1.subscribe_num)                  as subscribe_num, -- 认购量 
coalesce(t2.sign_num,t1.sign_num)                            as sign_num, -- 签约量 
null                                                         as returned_money, -- 回款 

coalesce(t1.first_clue_num,t2.first_clue_num)                as first_clue_num, -- 首次线索量 
coalesce(t2.first_distribute_num,t1.first_distribute_num)    as first_distribute_num, -- 首次上户量 
coalesce(t2.first_see_num,t1.first_see_num)                  as first_see_num, -- 首次带看量 
coalesce(t2.first_subscribe_num,t1.first_subscribe_num)      as first_subscribe_num, -- 首次认购量 
coalesce(t2.first_sign_num,t1.first_sign_num)                as first_sign_num, -- 首次签约量 
null                                                         as first_returned_money, -- 首次回款

t1.jump_num                                                  as jump_num, -- 跳出量 
t1.uv                                                        as uv, -- UV 

current_timestamp()                                           as etl_time,
t1.pdate                                                      as pdate,
'hasclue'                                                     as pclue 

from tmp_dev_1.tmp_sem_keyword_day_indi t1 
left join tmp_dev_1.tmp_market_offline_add_kw_indi t2 

on t1.channel_id = t2.channel_id 
and concat_ws('|',t1.plan_name,t1.unit_name,t1.keyword_name) = t2.channel_put 
and t1.pdate = t2.pdate 

and t2.channel_put != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and t2.channel_put != 'YD-通配-广州-1|低价|清远房价走势2019预测'

where t1.pdate <= '20190701'
;

quit;

