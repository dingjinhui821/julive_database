
set hive.execution.engine=spark;
set spark.app.name=app_base_head_customer;
set spark.yarn.queue=etl;
insert overwrite table julive_app.app_head_customer_base 

select 

t1.order_id                                          as clue_id,
t1.creator                                           as tag_creator_id, 
t4.emp_name                                          as tag_creator_name,
t4.direct_leader_id                                  as now_tag_creator_leader_id,
t4.direct_leader_name                                as now_tag_creator_leader_name,
t3.direct_leader_id                                  as tag_creator_leader_id,
t3.direct_leader_name                                as tag_creator_leader_name,
from_unixtime(t1.create_datetime,'yyyy-MM-dd')       as tag_create_date,

t2.channel_id                                        as channel_id,
t2.distribute_date                                   as distribute_date,
t2.customer_intent_city_id                           as customer_intent_city_id,
t2.customer_intent_city_name                         as customer_intent_city_name,
t2.customer_intent_city_seq                          as customer_intent_city_seq,
t2.intent_low_time                                   as intent_low_time,
t2.district_id                                       as district_id,
t2.interest_project                                  as interest_project,
t2.qualifications                                    as qualifications,
t2.intent_no_like                                    as intent_no_like,
t2.intent                                            as intent,
t2.intent_tc                                         as intent_tc,
t2.total_price_max                                   as total_price_max,
t2.budget_range_grade                                as budget_range_grade,
t2.create_date                                       as create_date,

t5.clue_num                                          as clue_num,
t5.distribute_num                                    as distribute_num,
t5.see_num                                           as see_num,
t5.see_project_num                                   as see_project_num,
t5.subscribe_num                                     as subscribe_num,
t5.subscribe_coop_num                                as subscribe_coop_num,
t5.sign_num                                          as sign_num,
t5.call_duration                                     as call_duration,
0                                                    as call_num,
-- 20210317 添加字段
case
when t2.from_source is not null then t2.from_source
else 1 end                                           as from_source,
t2.org_id                                            as org_id,
t2.org_name                                          as org_name,
current_timestamp()                                  as etl_time 

from ods.yw_order_tags t1 
-- 20210317 dim_clue_info替换成dim_clue_base_info
left join julive_dim.dim_clue_base_info t2 on t1.order_id = t2.clue_id
-- 20210317 dim_ps_employee_info 替换成dim_employee_base_info
left join julive_dim.dim_employee_base_info t3 on t1.creator = t3.emp_id and from_unixtime(t1.create_datetime,'yyyyMMdd') = t3.pdate  
-- 20210317 dim_ps_employee_info 替换成dim_employee_base_info
left join julive_dim.dim_employee_base_info t4 on t1.creator = t4.emp_id and t4.end_date = '9999-12-31' 
-- 20210317 fact_clue_full_line_indi 替换成fact_clue_full_line_base_indi
left join julive_fact.fact_clue_full_line_indi t5 on t1.order_id = t5.clue_id 
where t1.tag_id = 7 
;


-- 加工子表 


-- 居理数据 
insert overwrite table julive_app.app_head_customer 
select t.* 
from julive_app.app_head_customer_base t 
where t.from_source = 1 
;
-- 乌鲁木齐数据 
insert overwrite table julive_app.app_wlmq_head_customer 
select t.* 
from julive_app.app_head_customer_base t 
where t.from_source = 2 
;
-- 二手房中介数据 
insert overwrite table julive_app.app_esf_head_customer
select t.* 
from julive_app.app_head_customer_base t 
where t.from_source = 3 
;
-- 加盟商数据 
insert overwrite table julive_app.app_jms_head_customer 
select t.* 
from julive_app.app_head_customer_base t 
where t.from_source = 4 
;

insert into table julive_app.app_jms_head_customer 
select t.* 
from julive_app.app_head_customer_base t 
where t.from_source = 1 and t.org_id !=48 
;

