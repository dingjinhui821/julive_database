-- 市场关键词主题表 
-- julive_topic.topic_sem_keyword_hour_indi  

-- 传参,补数逻辑 
set etl_date = '${hiveconf:etldate}'; -- 取昨天日期 

------------------------------------------------------------------------------------------------------
-- 第一步：计算基础表
-- set etl_date = '20190714'; -- 取昨天日期 

set spark.app.name=topic_sem_keyword_hour_indi;
set hive.execution.engine=spark;
set mapreduce.job.queuename=root.etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

set spark.yarn.queue=etl;
set spark.executor.cores=1;
set spark.executor.memory=4g;
set spark.executor.instances=14;
set spark.yarn.executor.memoryOverhead=4096;

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
-- 1 计算线下指标 
drop table if exists tmp_dev_1.tmp_market_offline_hour_indi;
create table tmp_dev_1.tmp_market_offline_hour_indi as 

select 

min(t4.city_id) as city_id,
max(t4.city_name) as city_name, 

t1.channel_id,
max(t4.channel_name) as channel_name,

null as account_id,
null as account_name,

max(t4.media_id)     as media_id,
max(t4.media_name)   as media_name,
max(t4.module_id)    as module_id,
max(t4.module_name)  as module_name,

null as plan_id,
split(t1.channel_put,"\\|")[0] as plan_name,

null as unit_id,
split(t1.channel_put,"\\|")[1] as unit_name,

null as keyword_id,
split(t1.channel_put,"\\|")[2] as keyword_name,

t1.channel_put,
t1.clue_create_hour as report_hour,

-- 关键词粒度线下指标  
sum(t1.leave_phone_num) as leave_phone_num, -- 留电量 
count(t1.clue_id) as clue_num, -- 线索量 
count(if(t1.is_distribute=1,t1.clue_id,null)) as distribute_num, -- 上户量 
sum(t2.see_num) as see_num, -- 带看量 
sum(t3.subscribe_num) as subscribe_num, -- 认购量 

-- first关键词粒度线下指标  
count(if(t1.is_first_website = 1,t1.clue_id,null)) as first_clue_num, -- 首次线索量 
count(if(t1.is_first_website = 1 and t1.is_distribute = 1,t1.clue_id,null)) as first_distribute_num, -- 首次上户量 
sum(if(t1.is_first_website = 1,t2.see_num,null)) as first_see_num, -- 首次带看量 
sum(if(t1.is_first_website = 1,t3.subscribe_num,null)) as first_subscribe_num -- 首次认购量 

from (

select 

t.channel_id,
t.channel_put,
t.clue_id,
t.is_distribute,
regexp_replace(regexp_replace(substr(t.clue_create_time,1,13),'-',''),' ','') as clue_create_hour,
min(t.is_first_website) as is_first_website,
count(1) as leave_phone_num

from julive_fact.fact_site_behavior_dtl t 
where t.from_source = 1 
and t.channel_put is not null 
and t.channel_put != '' 

group by 
t.channel_id,
t.channel_put,
t.clue_id,
t.is_distribute,
regexp_replace(regexp_replace(substr(t.clue_create_time,1,13),'-',''),' ','')

) t1 left join (

select

order_id,
count(id) as see_num

from ods.yw_see_project
where status >= 40
and status < 60
group by order_id

) t2 on t1.clue_id = t2.order_id 
left join (

select 

order_id,
count(id) subscribe_num

from ods.yw_subscribe 
where subscribe_type in (1,4) 
and status in (1,2)
group by order_id

) t3 on t1.clue_id = t3.order_id 
join julive_dim.dim_channel_info t4 on t1.channel_id = t4.channel_id 

group by 
t1.channel_id,
t1.channel_put,
t1.clue_create_hour
;

-- ------------------------------------------------------------------------------------------------------------------
-- 2 计算指标结果表 
insert overwrite table julive_topic.topic_sem_keyword_hour_indi partition(pdate,pclue) 

select 

coalesce(tmp1.city_id,tmp2.city_id,tmp3.city_id)                                          as city_id, -- 城市 
coalesce(tmp1.city_name,tmp2.city_name,tmp3.city_name)                                    as city_name, 

coalesce(tmp1.media_id,tmp2.media_id,tmp3.media_id)                                       as media_id, -- 媒体 
coalesce(tmp1.media_name,tmp2.media_name,tmp3.media_name)                                 as media_name, -- 媒体名称 

coalesce(tmp1.module_id,tmp2.module_id,tmp3.module_id)                                    as module_id, -- 产品形态 
coalesce(tmp1.module_name,tmp2.module_name,tmp3.module_name)                              as module_name,

coalesce(tmp1.channel_id,tmp2.channel_id,tmp3.channel_id)                                 as channel_id, -- 渠道 
coalesce(tmp1.channel_name,tmp2.channel_name,tmp3.channel_name)                           as channel_name,

coalesce(tmp1.account_id,tmp3.account_id,tmp2.account_id)                                 as account_id, -- 账户 
coalesce(tmp1.account_name,tmp3.account_name,tmp2.account_name)                           as account_name,

coalesce(tmp1.plan_id,tmp3.plan_id)                                                       as plan_id, -- 计划 
coalesce(tmp1.plan_name,tmp2.plan_name,tmp3.plan_name)                                    as plan_name,

coalesce(tmp1.unit_id,tmp3.unit_id)                                                       as unit_id, -- 单元 
coalesce(tmp1.unit_name,tmp2.unit_name,tmp3.unit_name)                                    as unit_name,

coalesce(tmp1.keyword_id,tmp3.keyword_id)                                                 as keyword_id, -- 关键词 
coalesce(tmp1.keyword_name,tmp2.keyword_name,tmp3.keyword_name)                           as keyword_name,

coalesce(tmp1.report_hour,tmp2.report_hour)                                               as report_hour,

tmp1.show_num                                                                             as show_num, -- 展 
tmp1.click_num                                                                            as click_num, -- 点 
tmp1.cost                                                                                 as cost, -- 消 

-- 每个keyword 
tmp2.leave_phone_num                                                                      as leave_phone_num, -- 留电量 
tmp2.clue_num                                                                             as clue_num, -- 线索量 
tmp2.distribute_num                                                                       as distribute_num, -- 上户量 
tmp2.see_num                                                                              as see_num, -- 带看量
tmp2.subscribe_num                                                                        as subscribe_num, -- 认购量 
null                                                                                      as sign_num,
null                                                                                      as returned_money,

-- 首个keyword 
tmp2.first_clue_num                                                                       as first_clue_num,
tmp2.first_distribute_num                                                                 as first_distribute_num,
tmp2.first_see_num                                                                        as first_see_num,
tmp2.first_subscribe_num                                                                  as first_subscribe_num,
null                                                                                      as first_sign_num,
null                                                                                      as first_returned_money,

null, -- 预留字段 
null,
null,
null,
null,
null,
null,
null,
null,
null,

current_timestamp()                                                                       as etl_time,
coalesce(tmp1.pdate,substr(tmp2.report_hour,1,8))                                         as pdate,
if(tmp2.clue_num is not null,'hasclue','nohasclue')                                       as pclue

from (

select * 
from julive_fact.fact_dsp_sem_keyword_hour_indi 
where pdate = ${hiveconf:etl_date} 

) tmp1 full join (

select * 
from tmp_dev_1.tmp_market_offline_hour_indi 
where substr(report_hour,1,8) = ${hiveconf:etl_date}
and channel_put != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and channel_put != 'YD-通配-广州-1|低价|清远房价走势2019预测'

) tmp2 
on tmp1.channel_id = tmp2.channel_id 
and tmp1.channel_put = tmp2.channel_put 
and tmp1.report_hour = tmp2.report_hour 

left join ( -- 复用day【julive_topic.topic_sem_keyword_hour_indi】级别临时表 

select * 
from tmp_dev_1.tmp_before_keyword_base_info 
where pdate = ${hiveconf:etl_date}

) tmp3 
on tmp2.channel_id = tmp3.channel_id 
and tmp2.channel_put = tmp3.channel_put 
;

-- -------------------------------------------------------------------------------------------------
-- 3 备份产生指标数据  
insert overwrite table tmp_dev_1.tmp_sem_keyword_hour_indi partition(pdate) 

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
report_hour,

show_num,
click_num,
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

null as col1,
null as col2,
null as col3,
null as col4,
null as col5,
null as col6,
null as col7,
null as col8,
null as col9,
null as col10,

etl_time,
pdate 

from julive_topic.topic_sem_keyword_hour_indi 
where pdate = ${hiveconf:etl_date} 
and pclue = 'hasclue'
;

-- 4 刷新产生线索历史数据 
insert overwrite table julive_topic.topic_sem_keyword_hour_indi partition(pdate,pclue) 
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
coalesce(t1.report_hour,t2.report_hour)                      as report_hour,

t1.show_num                                                  as show_num, -- 展 
t1.click_num                                                 as click_num, -- 点 
t1.cost                                                      as cost, -- 消 
t1.leave_phone_num                                           as leave_phone_num, -- 留电量 

coalesce(t1.clue_num,t2.clue_num)                            as clue_num, -- 线索量 
coalesce(t2.distribute_num,t1.distribute_num)                as distribute_num, -- 上户量 
coalesce(t2.see_num,t1.see_num)                              as see_num, -- 带看量 
coalesce(t2.subscribe_num,t1.subscribe_num)                  as subscribe_num, -- 认购量 
null                                                         as sign_num, -- 签约量 
null                                                         as returned_money, -- 回款 

coalesce(t1.first_clue_num,t2.first_clue_num)                as first_clue_num, -- 首次线索量 
coalesce(t2.first_distribute_num,t1.first_distribute_num)    as first_distribute_num, -- 首次上户量 
coalesce(t2.first_see_num,t1.first_see_num)                  as first_see_num, -- 首次带看量 
coalesce(t2.first_subscribe_num,t1.first_subscribe_num)      as first_subscribe_num, -- 首次认购量 
null                                                         as first_sign_num, -- 首次签约量 
null                                                         as first_returned_money, -- 首次回款

null, -- 预留字段 
null,
null,
null,
null,
null,
null,
null,
null,
null,

current_timestamp()                                           as etl_time,
t1.pdate                                                      as pdate,
'hasclue'                                                     as pclue 

from tmp_dev_1.tmp_sem_keyword_hour_indi t1 
left join tmp_dev_1.tmp_market_offline_hour_indi t2 

on t1.channel_id = t2.channel_id 
and concat_ws('|',t1.plan_name,t1.unit_name,t1.keyword_name) = t2.channel_put 
and t1.report_hour = t2.report_hour 

and t2.channel_put != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and t2.channel_put != 'YD-通配-广州-1|低价|清远房价走势2019预测'

where t1.pdate <= '20190701'
;
quit;
