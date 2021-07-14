set mapred.job.name=app_appinstall_product_media_day;
set mapreduce.job.queuename=root.etl;

--临时表 媒体优先级utm_source->channel_type_name->media_name
drop table if exists tmp_dev_1.tmp_clue_distribute_see_subscribe_sign_num;
create table tmp_dev_1.tmp_clue_distribute_see_subscribe_sign_num as
  with tmp1 as (
  select 
  order_id                                               as clue_id,
  product_id                                             as product_id, -- 端 
  if(utm_source is not null,utm_source,channel)          as utm_source --媒体名称
  from app.app_marketing_user_channel_utm_mapping_test_v192 
  where is_last_install = 1 
  ),
  tmp2 as (
  select 
  a1.clue_id           as clue_id,
  a1.product_id        as product_id,
  a2.channel_type_name as media_name 

  from tmp1 a1 
  left join ods.cj_agency a2 on a1.utm_source = a2.utm_source 
  )
  select 
  t1.clue_id                                                as clue_id,
  t1.product_id                                             as product_id,
  if(t1.media_name is not null,t1.media_name,t3.media_name) as media_name
  from tmp2 t1 
  left join julive_dim.dim_clue_info t2 on t1.clue_id = t2.clue_id 
  left join julive_dim.dim_channel_info t3 on t2.channel_id = t3.channel_id 
  ;



insert overwrite table julive_app.app_appinstall_product_media_day 

select 

b1.create_date                             as create_date,
b1.product_id                              as product_id,
b1.city_id                                 as city_id,
b2.city_name                               as city_name,
b1.media_name                              as media_name,

sum(b1.clue_num)                           as clue_num,
sum(b1.distribute_num)                     as distribute_num,
max(b1.clue_see_num)                       as clue_see_num, 
sum(b1.see_num)                            as see_num,
max(b1.clue_subscribe_num)                 as clue_subscribe_num,
sum(b1.subscribe_num)                      as subscribe_num,
max(b1.clue_sign_num)                      as clue_sign_num,
sum(b1.sign_num)                           as sign_num,

current_timestamp()                        as etl_time 

from (

select 
  to_date(t2.create_time)                    as create_date,
  t1.product_id                              as product_id,
  t2.customer_intent_city_id                 as city_id,
  t1.media_name                              as media_name,
  count(t2.clue_id)                          as clue_num,  --线索量
  0                                          as distribute_num,
  0                                          as clue_see_num,
  0                                          as see_num,
  0                                          as clue_subscribe_num,
  0                                          as subscribe_num,
  0                                          as clue_sign_num,
  0                                          as sign_num
  from julive_dim.dim_clue_info t2 
  join tmp_dev_1.tmp_clue_distribute_see_subscribe_sign_num t1 
  on t1.clue_id = t2.clue_id 
  group by 
  t1.product_id,to_date(t2.create_time),t2.customer_intent_city_id,t1.media_name
  
union all

select 

  to_date(t2.distribute_date)         as create_date,
  t1.product_id                       as product_id,
  t2.customer_intent_city_id          as city_id,
  t1.media_name                       as media_name,
  0                                   as clue_num,
  count(t2.clue_id)                   as distribute_num, --上户量
  0                                   as clue_see_num,
  0                                   as see_num,
  0                                   as clue_subscribe_num,
  0                                   as subscribe_num,
  0                                   as clue_sign_num,
  0                                   as sign_num
  from 
  julive_dim.dim_clue_info t2  
  join tmp_dev_1.tmp_clue_distribute_see_subscribe_sign_num t1 on t1.clue_id = t2.clue_id 
  where t2.is_distribute = 1 
  group by 
  t1.product_id,to_date(t2.distribute_date),t2.customer_intent_city_id,t1.media_name
  
union all

select 

  to_date(t2.see_create_time)                 as create_date,
  t1.product_id                               as product_id,
  t2.project_city_id                          as city_id,
  t1.media_name                               as media_name,
  0                                           as clue_num,
  0                                           as distribute_num,
  count(distinct t2.clue_id)                  as clue_see_num, --产生带看的线索量
  sum(t2.see_num)                             as see_num, --带看量 
  0                                           as clue_subscribe_num,
  0                                           as subscribe_num,
  0                                           as clue_sign_num,
  0                                           as sign_num
  from  
  julive_fact.fact_see_project_dtl t2 
  join 
  tmp_dev_1.tmp_clue_distribute_see_subscribe_sign_num t1
  on 
  t1.clue_id = t2.clue_id

  group by 
  t1.product_id,to_date(t2.see_create_time),t2.project_city_id,t1.media_name
  
union all

select 
  to_date(t2.subscribe_time)                                as create_date,
  t1.product_id                                             as product_id,
  t2.project_city_id                                        as city_id,
  t1.media_name                                             as media_name,
  0                                                         as clue_num,
  0                                                         as distribute_num,
  0                                                         as clue_see_num,
  0                                                         as see_num,
  count(distinct t2.clue_id)                                as clue_subscribe_num,
  sum(t2.subscribe_contains_cancel_ext_num)                 as subscribe_num, --认购量
  0                                                         as clue_sign_num,
  0                                                         as sign_num
  from 
  julive_fact.fact_subscribe_dtl t2  
  join 
  tmp_dev_1.tmp_clue_distribute_see_subscribe_sign_num t1
  on 
  t2.clue_id = t1.clue_id
  group by 
  to_date(t2.subscribe_time),t1.product_id,t2.project_city_id,t1.media_name
  
union all

select 
  to_date(t2.sign_time)                           as create_date,
  t1.product_id                                   as product_id,
  t2.project_city_id                              as city_id,
  t1.media_name                                   as media_name,
  0                                               as clue_num,
  0                                               as distribute_num,
  0                                               as clue_see_num,
  0                                               as see_num,
  0                                               as clue_subscribe_num,
  0                                               as subscribe_num,
  count(distinct(t2.clue_id))                     as clue_sign_num,
  sum(t2.sign_contains_cancel_ext_num)            as sign_num --签约量
  from 
  julive_fact.fact_sign_dtl t2 
  join 
  tmp_dev_1.tmp_clue_distribute_see_subscribe_sign_num t1
  on 
  t2.clue_id = t1.clue_id
  group by 
  to_date(t2.sign_time),t1.product_id,t2.project_city_id,t1.media_name
) b1 
left join julive_dim.dim_city b2 on b1.city_id = b2.city_id 
 group by 
 b1.create_date,
 b1.product_id,
 b1.city_id,
 b2.city_name,
 b1.media_name
 ;

