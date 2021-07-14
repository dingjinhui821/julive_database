-- ----------------------------------------------
set hive.execution.engine=spark;
set spark.app.name=fact_clue_full_line_base_indi;
set spark.yarn.queue=etl;
insert overwrite table julive_fact.fact_clue_full_line_base_indi 

select 

-- 维度 
t1.clue_id,
t1.org_id,
t1.org_type,
t1.org_name,
"" as developer_id,
"" as project_id,
t1.op_type,
t1.op_type_name_cn,
t1.city_id,
t1.city_name,
t1.city_seq,
t1.customer_intent_city_id,
t1.customer_intent_city_name,
t1.customer_intent_city_seq,

t1.first_customer_intent_city_id,
t1.first_customer_intent_city_name,
t1.first_customer_intent_city_seq,

t1.source,
t1.source_tc,
t1.user_id,
t1.user_name,
t1.user_mobile      as user_mobile,
t1.is_distribute,
t1.emp_id,        
t1.emp_name,      
t1.channel_id,
channel.city_id     as  channel_city_id,
channel.city_name   as  channel_city_name,
channel.city_seq    as  channel_city_seq,
channel.media_id,
channel.media_name,
channel.module_id,
channel.module_name,
channel.device_id,
channel.device_name,
to_date(t1.create_time) as create_date,
t1.distribute_date,
t1.distribute_time,
t2.first_see_date,
t2.final_see_date,
t3.first_subscribe_date,
t3.final_subscribe_date,
t4.first_sign_date,
t4.final_sign_date,

-- 指标 
1                                                            as clue_num, -- 线索量 
if(t1.is_distribute = 1,1,0)                                 as distribute_num, -- 上户量 

coalesce(t2.see_num,0)                                       as see_num, -- 带看量 
coalesce(t2.see_project_num,0)                               as see_project_num, -- 带看楼盘量 

coalesce(t3.subscribe_contains_cancel_ext_num,0)             as subscribe_num, -- 认购量，退+外联 
coalesce(t3.subscribe_coop_num,0)                            as subscribe_coop_num, -- 净认购量 
coalesce(t3.subscribe_contains_cancel_ext_num,0)             as subscribe_contains_cancel_ext_num, -- 认购量，退+外联 
coalesce(t3.subscribe_contains_ext_num)                      as subscribe_contains_ext_num, -- 认购量 不含退、含外联 
coalesce(t3.subscribe_contains_ext_amt,0)                    as subscribe_contains_ext_amt, -- 认购金额 含外联 
coalesce(t3.subscribe_contains_ext_income,0)                 as subscribe_contains_ext_income, -- 认购佣金 含外联 
coalesce(t3.subscribe_contains_cancel_ext_amt,0)             as subscribe_contains_cancel_ext_amt, -- 认购金额 含退 含外联 
coalesce(t3.subscribe_contains_cancel_ext_income,0)          as subscribe_contains_cancel_ext_income, -- 认购金额 含退 含外联 
coalesce(t4.sign_contains_cancel_ext_num,0)                  as sign_num, -- 签约量 , 退+外联 

coalesce(t5.call_duration,0)                                 as call_duration, -- 订单通话时长 
coalesce(t5.call_num,0)                                      as call_num, -- 订单通话时长 

-- 李想需求 
t1.clue_id                                                   as clue_id_list,
if(t1.is_distribute = 1,t1.clue_id,null)                     as distribute_id_list,

t2.see_id_list                                                                                                     as see_id_list,
t2.see_project_id_list                                                                                             as see_project_id_list,
if(t2.see_num is not null and t2.see_num != 0,t1.clue_id,null)                                                     as clue_see_list,

t3.subscribe_contains_cancel_ext_id_list                                                                           as subscribe_contains_cancel_ext_id_list,
if(t3.subscribe_contains_cancel_ext_num is not null and t3.subscribe_contains_cancel_ext_num != 0,t1.clue_id,null) as clue_subscribe_list,

t4.sign_contains_cancel_ext_id_list                                                                                as sign_contains_cancel_ext_id_list,
if(t4.sign_contains_cancel_ext_num is not null and t4.sign_contains_cancel_ext_num != 0,t1.clue_id,null)           as clue_sign_list,

if(t2.see_num is not null and t2.see_num != 0,1,0)                                                                 as clue_see_num,
if(t3.subscribe_contains_cancel_ext_num is not null and t3.subscribe_contains_cancel_ext_num != 0,1,0)             as clue_subscribe_num,
if(t4.sign_contains_cancel_ext_num is not null and t4.sign_contains_cancel_ext_num != 0,1,0)                       as clue_sign_num,

coalesce(t1.create_time,0)                                   as create_time,
t1.from_source                                               as from_source,
current_timestamp()                                          as etl_time 

-- Join逻辑 ------------------------------------------------------------------------------------
from julive_dim.dim_clue_base_info t1 
left join julive_dim.dim_channel_info channel on t1.channel_id = channel.channel_id 
left join ( -- 线索维度 带看量，带看楼盘量 

select 

clue_id,
from_source,
to_date(min(if(status >= 40 and status < 60,plan_real_begin_time,null)))                         as first_see_date,
to_date(max(if(status >= 40 and status < 60,plan_real_begin_time,null)))                         as final_see_date,
sum(see_num)                                                                                     as see_num,
sum(see_project_num)                                                                             as see_project_num,
concat_ws(',',collect_set(if(status >= 40 and status < 60,cast(see_id as string),null)))         as see_id_list,
concat_ws(',',collect_set(if(status >= 40 and status < 60,cast(see_project_id as string),null))) as see_project_id_list


from julive_fact.fact_see_project_base_dtl 
group by from_source,clue_id 

) t2 on t1.clue_id = t2.clue_id and t1.from_source =t2.from_source
left join ( -- 线索维度 认购量,认购量-含退、含外联

select 

clue_id,
from_source,
to_date(min(subscribe_time))                                                                                            as first_subscribe_date,
to_date(max(subscribe_time))                                                                                            as final_subscribe_date,
sum(subscribe_contains_cancel_ext_num)                                                                                  as subscribe_contains_cancel_ext_num, -- 含退含外联 
sum(subscribe_coop_num)                                                                                                 as subscribe_coop_num, -- 净认购量 
sum(subscribe_contains_ext_num)                                                                                         as subscribe_contains_ext_num, -- 认购量-不含退含外联 
sum(subscribe_contains_ext_amt)                                                                                         as subscribe_contains_ext_amt,
sum(subscribe_contains_ext_income)                                                                                      as subscribe_contains_ext_income,
sum(subscribe_contains_cancel_ext_amt)                                                                                  as subscribe_contains_cancel_ext_amt,
sum(subscribe_contains_cancel_ext_income)                                                                               as subscribe_contains_cancel_ext_income,
concat_ws(',',collect_set(if(subscribe_status in (1,2) and subscribe_type in (1,4),cast(subscribe_id as string),null))) as subscribe_contains_cancel_ext_id_list

from julive_fact.fact_subscribe_base_dtl 
group by from_source,clue_id 

) t3 on t1.clue_id = t3.clue_id and t1.from_source =t3.from_source
left join ( -- 线索维度 签约量-含退、含外联

select 

clue_id,
from_source,
to_date(min(sign_time))                                                                                  as first_sign_date,
to_date(max(sign_time))                                                                                  as final_sign_date,
sum(sign_contains_cancel_ext_num)                                                                        as sign_contains_cancel_ext_num,
concat_ws(',',collect_set(if(sign_status in (1,2) and sign_type in (1,4),cast(sign_id as string),null))) as sign_contains_cancel_ext_id_list 

from julive_fact.fact_sign_base_dtl 
group by from_source,clue_id 

) t4 on t1.clue_id = t4.clue_id and t1.from_source =t4.from_source
left join ( -- 通话次数、通话时长 

select 

clue_id,
sum(call_duration)                                                                                       as call_duration, -- 通话时长 
count(1)                                                                                                 as call_num  -- 通话次数 

from julive_fact.fact_consultant_call_log_dtl 
group by clue_id 

) t5 on t1.clue_id = t5.clue_id 
;


insert overwrite table julive_fact.fact_clue_full_line_indi
select * from julive_fact.fact_clue_full_line_base_indi
where from_source = 1;

insert overwrite table julive_fact.fact_wlmq_clue_full_line_indi
select * from julive_fact.fact_clue_full_line_base_indi
where from_source = 2;

insert overwrite table julive_fact.fact_esf_clue_full_line_indi
select * from julive_fact.fact_clue_full_line_base_indi
where from_source = 3;

insert overwrite table julive_fact.fact_jms_clue_full_line_indi
select * from julive_fact.fact_clue_full_line_base_indi
where from_source = 4;

insert into table julive_fact.fact_jms_clue_full_line_indi
select * from julive_fact.fact_clue_full_line_base_indi
where from_source = 1 and org_id!=48;



set hive.execution.engine=spark; 
insert overwrite table julive_fact.fact_kfsclue_full_line_indi 

select 

t1.id                                 as clue_id,
-- 2                                     as from_source,
''                                    as org_id,
''                                    as org_type,
''                                    as org_name,
t1.developer_id                       as developer_id,
t1.project_id                         as project_id,
t1.op_type                            as op_type,
t2.op_type_name_cn                    as op_type_name_cn,
t1.city_id                            as city_id,
t3.city_name                          as city_name,
t3.city_seq                           as city_seq,
''                                    as customer_intent_city_id,
''                                    as customer_intent_city_name,
''                                    as customer_intent_city_seq,
''                                    as first_customer_intent_city_id,
''                                    as first_customer_intent_city_name,
''                                    as first_customer_intent_city_seq,
t1.source                             as source,
case 
when t1.source = 1 then '小程序留电' 
when t1.source = 1 then 'H5留电' 
when t1.source = 1 then '人工录入' 
end                                   as source_tc,
t1.user_id                            as user_id,
t1.user_realname                      as user_name,
t1.user_mobile                        as user_mobile,
''                                    as emp_id,
''                                    as emp_name,        
1                                     as is_distribute,
t1.channel_id                         as channel_id, 
t4.city_id                            as  channel_city_id,
t4.city_name                          as  channel_city_name,
t4.city_seq                           as  channel_city_seq,
t4.media_id                           as media_id,
t4.media_name                         as media_name,
t4.module_id                          as module_id,
t4.module_name                        as module_name,
t4.device_id                          as device_id,
t4.device_name                        as device_name,

to_date(from_unixtime(t1.create_datetime))                            as create_date,
to_date(from_unixtime(t1.distribute_datetime))                        as distribute_date,
from_unixtime(t1.distribute_datetime,'yyyy-MM-dd HH:mm:ss')           as distribute_time,
''                                                                    as first_see_date,
''                                                                    as final_see_date,
''                                                                    as first_subscribe_date,
''                                                                    as final_subscribe_date,
''                                                                    as first_sign_date,
''                                                                    as final_sign_date,
1                                                                     as clue_num,
1                                                                     as distribute_num,
null                                                                  as see_num,
null                                                                  as see_project_num,
null                                                                  as subscribe_num,
null                                                                  as subscribe_coop_num,
null                                                                  as subscribe_contains_cancel_ext_num,
null                                                                  as subscribe_contains_ext_num,
null                                                                  as subscribe_contains_ext_amt,
null                                                                  as subscribe_contains_ext_income,
null                                                                  as subscribe_contains_cancel_ext_amt,
null                                                                  as subscribe_contains_cancel_ext_income,
null                                                                  as sign_num,
t1.talking_duration_count                                             as call_duration,
t1.talking_count                                                      as call_num,
null                                                                  as clue_id_list,
null                                                                  as distribute_id_list,
null                                                                  as see_id_list,
null                                                                  as see_project_id_list,
null                                                                  as clue_see_list,
null                                                                  as subscribe_contains_cancel_ext_id_list,
null                                                                  as clue_subscribe_list,
null                                                                  as sign_contains_cancel_ext_id_list,
null                                                                  as clue_sign_list,
null                                                                  as clue_see_num,
null                                                                  as clue_subscribe_num,
null                                                                  as clue_sign_num,
from_unixtime(t1.create_datetime)                                     as create_time, 
10                                                                    as from_source,
current_timestamp()                                                   as etl_time 

from ods.kfs_order t1 
left join julive_dim.dim_optype t2 on t1.op_type = t2.skey 
left join julive_dim.dim_city t3 on t1.city_id = t3.city_id -- 替换 
left join julive_dim.dim_channel_info t4 on t1.channel_id = t4.channel_id 

where t1.developer_id != 822 -- 剔除测试数据 
;



