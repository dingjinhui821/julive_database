-- 市场关键词主题表 
-- julive_topic.topic_sem_keyword_hour_indi  

-- 传参,补数逻辑 
set etl_date = '${hiveconf:etlDate}'; -- 取昨天日期 

------------------------------------------------------------------------------------------------------
-- 第一步：计算基础表
-- set etl_date = '20190904'; -- 取昨天日期 

set hive.warehouse.subdir.inherit.perms=false;
set spark.app.name=topic_sem_keyword_hour_indi;
set hive.execution.engine=spark;
set mapreduce.job.queuename=root.etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
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
if(max(if(t1.keyword_id != -1 or t1.keyword_id is not null,1,0))=0,split(t1.channel_put2,"\\|")[0],null) as plan_name,

null as unit_id,
if(max(if(t1.keyword_id != -1 or t1.keyword_id is not null,1,0))=0,split(t1.channel_put2,"\\|")[1],null) as unit_name,

if(max(if(t1.keyword_id != -1 or t1.keyword_id is not null,1,0))=0,null,max(t1.keyword_id)) as keyword_id,
if(max(if(t1.keyword_id != -1 or t1.keyword_id is not null,1,0))=0,split(t1.channel_put2,"\\|")[2],null) as keyword_name,

max(if(t1.keyword_id != -1 or t1.keyword_id is not null,1,0)) as has_keyword_id,
t1.channel_put2,
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

tmp.channel_id,
if(tmp.keyword_id = -1 or tmp.keyword_id is null,tmp.channel_put,tmp.keyword_id) as channel_put2,
tmp.keyword_id as keyword_id,
tmp.channel_put as channel_put,
tmp.clue_id,
tmp.is_distribute,
tmp.clue_create_hour,
tmp.is_first_website,
tmp.leave_phone_num

from (

select 

t.channel_id,
t.clue_id,
t.is_distribute,
regexp_replace(regexp_replace(substr(t.clue_create_time,1,13),'-',''),' ','') as clue_create_hour,
t.channel_put,
max(t.keyword_id) as keyword_id,
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

) tmp

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
t1.channel_put2,
t1.clue_create_hour 
;

-- ------------------------------------------------------------------------------------------------------------------
-- 2 计算指标结果表 ：join外部数据和业务数据逻辑
insert overwrite table julive_topic.topic_sem_keyword_hour_indi partition(pdate,pclue) 

select 

coalesce(tmp1.city_id,tmp2.city_id,tmp5.city_id,tmp4.city_id)                                              as city_id, -- 城市 
coalesce(tmp1.city_name,tmp2.city_name,tmp5.city_name,tmp4.city_name)                                      as city_name, 

coalesce(tmp1.media_id,tmp2.media_id,tmp5.media_id,tmp4.media_id,tmp6.media_id)                            as media_id, -- 媒体 
coalesce(tmp1.media_name,tmp2.media_name,tmp5.media_name,tmp4.media_name,tmp6.media_name)                  as media_name, -- 媒体名称 

coalesce(tmp1.module_id,tmp2.module_id,tmp5.module_id,tmp4.module_id,tmp6.module_id)                       as module_id, -- 产品形态 
coalesce(tmp1.module_name,tmp2.module_name,tmp5.module_name,tmp4.module_name,tmp6.module_name)             as module_name,

coalesce(tmp1.channel_id,tmp2.channel_id,tmp5.channel_id,tmp4.channel_id)                                  as channel_id, -- 渠道 
coalesce(tmp1.channel_name,tmp2.channel_name,tmp5.channel_name,tmp4.channel_name)                          as channel_name,

coalesce(tmp1.account_id,tmp4.account_id,tmp6.account_id,tmp2.account_id,tmp5.account_id)                  as account_id, -- 账户 
coalesce(tmp1.account_name,tmp4.account_name,tmp6.account_name,tmp2.account_name,tmp5.account_name)        as account_name,

coalesce(tmp1.plan_id,tmp4.plan_id,tmp6.plan_id)                                                           as plan_id, -- 计划 
coalesce(tmp1.plan_name,tmp2.plan_name,tmp5.plan_name,tmp4.plan_name,tmp6.plan_name)                       as plan_name,

coalesce(tmp1.unit_id,tmp4.unit_id,tmp6.unit_id)                                                           as unit_id, -- 单元 
coalesce(tmp1.unit_name,tmp2.unit_name,tmp5.unit_name,tmp4.unit_name,tmp6.unit_name)                       as unit_name,

coalesce(tmp1.keyword_id,tmp4.keyword_id,tmp6.keyword_id)                                                  as keyword_id, -- 关键词 
coalesce(tmp1.keyword_name,tmp2.keyword_name,tmp5.keyword_name,tmp4.keyword_name,tmp6.keyword_name)        as keyword_name,

coalesce(tmp1.report_hour,tmp2.report_hour,tmp5.report_hour)                                               as report_hour,

tmp1.show_num                                                                                              as show_num, -- 展 
tmp1.click_num                                                                                             as click_num, -- 点 
tmp1.cost                                                                                                  as cost, -- 消 

-- 每个keyword 
coalesce(tmp2.leave_phone_num,tmp5.leave_phone_num)                                                        as leave_phone_num, -- 留电量 
coalesce(tmp2.clue_num,tmp5.clue_num)                                                                      as clue_num, -- 线索量 
coalesce(tmp2.distribute_num,tmp5.distribute_num)                                                          as distribute_num, -- 上户量 
coalesce(tmp2.see_num,tmp5.see_num)                                                                        as see_num, -- 带看量
coalesce(tmp2.subscribe_num,tmp5.subscribe_num)                                                            as subscribe_num, -- 认购量 
null                                                                                                       as sign_num,
null                                                                                                       as returned_money,

-- 首个keyword 
coalesce(tmp2.first_clue_num,tmp5.first_clue_num)                                                          as first_clue_num,
coalesce(tmp2.first_distribute_num,tmp5.first_distribute_num)                                              as first_distribute_num,
coalesce(tmp2.first_see_num,tmp5.first_see_num)                                                            as first_see_num,
coalesce(tmp2.first_subscribe_num,tmp5.first_subscribe_num)                                                as first_subscribe_num,
null                                                                                                       as first_sign_num,
null                                                                                                       as first_returned_money,

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

current_timestamp()                                                                                       as etl_time,
coalesce(tmp1.pdate,substr(tmp2.report_hour,1,8),substr(tmp5.report_hour,1,8))                            as pdate,
if(tmp2.clue_num is not null or tmp5.clue_num is not null,'hasclue','nohasclue')                          as pclue

from (

select * 
from julive_fact.fact_dsp_sem_keyword_hour_indi 
where pdate = ${hiveconf:etl_date} 

) tmp1 
full join (

select * 
from tmp_dev_1.tmp_market_offline_hour_indi 
where substr(report_hour,1,8) = ${hiveconf:etl_date}
and has_keyword_id = 1 
and channel_put2 != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and channel_put2 != 'YD-通配-广州-1|低价|清远房价走势2019预测'

) tmp2 
on tmp1.channel_id = tmp2.channel_id 
and tmp1.keyword_id = tmp2.channel_put2 
and tmp1.report_hour = tmp2.report_hour 

full join (

select * 
from tmp_dev_1.tmp_market_offline_hour_indi 
where substr(report_hour,1,8) = ${hiveconf:etl_date}
and has_keyword_id = 0  
and channel_put2 != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and channel_put2 != 'YD-通配-广州-1|低价|清远房价走势2019预测'

) tmp5 
on tmp1.channel_id = tmp5.channel_id 
and tmp1.channel_put = tmp5.channel_put2 
and tmp1.report_hour = tmp5.report_hour 

left join (

select * 
from tmp_dev_1.tmp_before_keyword_base_info 
where pdate = ${hiveconf:etl_date}

) tmp4 
on tmp5.channel_id = tmp4.channel_id 
and tmp5.channel_put2 = tmp4.channel_put 

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

) tmp6 
on tmp2.channel_put2 = tmp6.keyword_id 
;

-- 迁移线下分区指标 
drop table tmp_dev_1.tmp_keyword_hour_temp_a;
create table tmp_dev_1.tmp_keyword_hour_temp_a as

select

a.city_id       as city_id,
a.city_name     as city_name,
a.media_id      as media_id,
a.media_name    as media_name,
a.module_id     as module_id,
a.module_name   as module_name,
a.channel_id    as channel_id,
a.channel_name  as channel_name,
a.account_id    as account_id,
a.account_name  as account_name,
a.plan_id       as plan_id,
a.plan_name     as plan_name,
a.unit_id       as unit_id,
a.unit_name     as unit_name,
a.keyword_id    as keyword_id,
a.keyword_name  as keyword_name,
a.report_hour   as report_hour,
a.show_num      as show_num,
a.click_num     as click_num,
a.cost          as cost,

case when b.leave_phone_num is not null then
    (case when a.leave_phone_num is null then 0 else a.leave_phone_num end) +(case when b.leave_phone_num is null then 0 else b.leave_phone_num end)
     when b.leave_phone_num is null then 0 else a.leave_phone_num end                                                                                 as leave_phone_num,
case when b.leave_phone_num is not null then
    (case when a.clue_num is null then 0 else a.clue_num end) +(case when b.clue_num is null then 0 else b.clue_num end)
     when b.leave_phone_num is null then 0 else a.clue_num end                                                                                        as clue_num,
case when b.leave_phone_num is not null then
    (case when a.distribute_num is null then 0 else a.distribute_num end) +(case when b.distribute_num is null then 0 else b.distribute_num end)
     when b.leave_phone_num is null then 0 else a.distribute_num end                                                                                  as distribute_num,
case when b.leave_phone_num is not null then
    (case when a.see_num is null then 0 else a.see_num end) +(case when b.see_num is null then 0 else b.see_num end)
     when b.leave_phone_num is null then 0 else a.see_num end                                                                                         as see_num,
case when b.leave_phone_num is not null then
    (case when a.subscribe_num is null then 0 else a.subscribe_num end) +(case when b.subscribe_num is null then 0 else b.subscribe_num end)
     when b.leave_phone_num is null then 0 else a.subscribe_num end                                                                                   as subscribe_num,
case when b.leave_phone_num is not null then
    (case when a.sign_num is null then 0 else a.sign_num end) +(case when b.sign_num is null then 0 else b.sign_num end)
     when b.leave_phone_num is null then 0 else a.sign_num end                                                                                        as sign_num,
case when b.leave_phone_num is not null then
    (case when a.returned_money is null then 0 else a.returned_money end) +(case when b.returned_money is null then 0 else b.returned_money end)
     when b.leave_phone_num is null then 0 else a.returned_money end                                                                                  as returned_money,
case when b.leave_phone_num is not null then
    (case when a.first_clue_num is null then 0 else a.first_clue_num end) +(case when b.first_clue_num is null then 0 else b.first_clue_num end)
     when b.leave_phone_num is null then 0 else a.first_clue_num end as first_clue_num,
case when b.leave_phone_num is not null then
    (case when a.first_distribute_num is null then 0 else a.first_distribute_num end) +(case when b.first_distribute_num is null then 0 else b.first_distribute_num end)
     when b.leave_phone_num is null then 0 else a.first_distribute_num end as first_distribute_num,
case when b.leave_phone_num is not null then
    (case when a.first_see_num is null then 0 else a.first_see_num end) +(case when b.first_see_num is null then 0 else b.first_see_num end)
     when b.leave_phone_num is null then 0 else a.first_see_num end as first_see_num,
case when b.leave_phone_num is not null then
    (case when a.first_subscribe_num is null then 0 else a.first_subscribe_num end) +(case when b.first_subscribe_num is null then 0 else b.first_subscribe_num end)
     when b.leave_phone_num is null then 0 else a.first_subscribe_num end as first_subscribe_num,
case when b.leave_phone_num is not null then
    (case when a.first_sign_num is null then 0 else a.first_sign_num end) +(case when b.first_sign_num is null then 0 else b.first_sign_num end)
     when b.leave_phone_num is null then 0 else a.first_sign_num end as first_sign_num,
case when b.leave_phone_num is not null then
    (case when a.first_returned_money is null then 0 else a.first_returned_money end) +(case when b.first_returned_money is null then 0 else b.first_returned_money end)
     when b.leave_phone_num is null then 0 else a.first_returned_money end as first_returned_money,
a.col1                 as col1,
a.col2                 as col2,
a.col3                 as col3,
a.col4                 as col4,
a.col5                 as col5,
a.col6                 as col6,
a.col7                 as col7,
a.col8                 as col8,
a.col9                 as col9,
a.col10                as col10,
a.etl_time             as etl_time,
a.pdate                as pdate,

case when b.clue_num is not null then 'hasclue'
 when b.clue_num is null then 'nohasclue'
else 'nohasclue' end   as pclue

from (
select * 
from julive_topic.topic_sem_keyword_hour_indi
where pdate =${hiveconf:etl_date} 
) a left join (
select * 
from julive_topic.topic_sem_keyword_hour_indi
where pdate = ${hiveconf:etl_date} 
and click_num < 1 
and leave_phone_num > 0
) b on a.city_id = b.city_id 
   and a.city_name = b.city_name
   and a.media_id = b.media_id
   and a.media_name = b.media_name
   and a.module_id = b.module_id
   and a.module_name = b.module_name
   and a.channel_id = b.channel_id
   and a.channel_name = b.channel_name
   and a.account_id = b.account_id
   and a.account_name = b.account_name
   and a.plan_id = b.plan_id
   and a.plan_name = b.plan_name
   and a.unit_id = b.unit_id
   and a.unit_name = b.unit_name
   and a.keyword_id = b.keyword_id
   and a.keyword_name = b.keyword_name
   and a.pdate = b.pdate
   and a.report_hour + 1 = b.report_hour
;

insert overwrite table julive_topic.topic_sem_keyword_hour_indi partition(pdate,pclue) 
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

leave_phone_num,
clue_num,
distribute_num,
see_num,
subscribe_num,
sign_num,
returned_money,

first_clue_num,
first_distribute_num,
first_see_num,
first_subscribe_num,
first_sign_num,
first_returned_money,

'' as col1,
'' as col2,
'' as col3,
'' as col4,
'' as col5,
'' as col6,
'' as col7,
'' as col8,
'' as col9,
'' as col10,
etl_time,
pdate,
pclue 

from tmp_dev_1.tmp_keyword_hour_temp_a
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
coalesce(t1.report_hour,t2.report_hour,t3.report_hour)                               as report_hour,

t1.show_num                                                                          as show_num, -- 展 
t1.click_num                                                                         as click_num, -- 点 
t1.cost                                                                              as cost, -- 消 
t1.leave_phone_num                                                                   as leave_phone_num, -- 留电量 

coalesce(t1.clue_num,t2.clue_num,t3.clue_num)                                        as clue_num, -- 线索量 
coalesce(t2.distribute_num,t3.distribute_num,t1.distribute_num)                      as distribute_num, -- 上户量 
coalesce(t2.see_num,t3.see_num,t1.see_num)                                           as see_num, -- 带看量 
coalesce(t2.subscribe_num,t3.subscribe_num,t1.subscribe_num)                         as subscribe_num, -- 认购量 
null                                                                                 as sign_num, -- 签约量 
null                                                                                 as returned_money, -- 回款 

coalesce(t1.first_clue_num,t2.first_clue_num,t3.first_clue_num)                      as first_clue_num, -- 首次线索量 
coalesce(t2.first_distribute_num,t3.first_distribute_num,t1.first_distribute_num)    as first_distribute_num, -- 首次上户量 
coalesce(t2.first_see_num,t3.first_see_num,t1.first_see_num)                         as first_see_num, -- 首次带看量 
coalesce(t2.first_subscribe_num,t3.first_subscribe_num,t1.first_subscribe_num)       as first_subscribe_num, -- 首次认购量 
null                                                                                 as first_sign_num, -- 首次签约量 
null                                                                                 as first_returned_money, -- 首次回款

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

from (

select * 
from tmp_dev_1.tmp_sem_keyword_hour_indi 
where pdate >= '20190702' 

) t1 left join (

select * 
from tmp_dev_1.tmp_market_offline_hour_indi 
where has_keyword_id = 1 
and channel_put2 != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and channel_put2 != 'YD-通配-广州-1|低价|清远房价走势2019预测'

) t2 
on t1.channel_id = t2.channel_id 
and t1.keyword_id = t2.channel_put2 
and t1.report_hour = t2.report_hour 

left join (

select * 
from tmp_dev_1.tmp_market_offline_hour_indi 
where has_keyword_id = 0 
and channel_put2 != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and channel_put2 != 'YD-通配-广州-1|低价|清远房价走势2019预测'

) t3 
on t1.channel_id = t3.channel_id 
and concat_ws('|',t1.plan_name,t1.unit_name,t1.keyword_name) = t3.channel_put2 
and t1.report_hour = t3.report_hour 
;

quit;
