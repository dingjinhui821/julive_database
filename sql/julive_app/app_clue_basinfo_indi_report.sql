
set hive.execution.engine=spark;
set spark.app.name=app_clue_basinfo_indi_report;
set spark.yarn.queue=etl;
insert overwrite table julive_app.app_clue_basinfo_indi_report 

select 

t1.clue_id,
t1.channel_id,
t3.channel_name,
t3.media_id,
t3.media_name,
t3.module_id,
t3.module_name,
t3.device_id,
t3.device_name,
t1.city_id,
t1.city_name,
t1.city_seq,
t1.customer_intent_city_id,
t1.customer_intent_city_name,
t1.customer_intent_city_seq,
t1.emp_id,
t1.emp_name,

t4.direct_leader_id       as now_direct_leader_id,
t4.direct_leader_name     as now_direct_leader_name,
t4.indirect_leader_id     as now_indirect_leader_id,
t4.indirect_leader_name   as now_indirect_leader_name,

t5.direct_leader_id,
t5.direct_leader_name,
t5.indirect_leader_id,
t5.indirect_leader_name,

t1.is_distribute,
t1.distribute_time,
t1.distribute_date,
t1.district_id,
t1.total_price_max,
t1.budget_range_grade,
t1.interest_project,
t1.investment,
t1.qualifications,
t1.intent,
t1.intent_low_time,
t1.purchase_purpose,
t1.purchase_purpose_tc,
t1.purchase_urgency,

t2.clue_num,
t2.distribute_num,
t2.see_num,
t2.see_project_num,
t2.subscribe_num,
t2.subscribe_coop_num,
t2.sign_num,
t2.call_duration,
t2.call_num,
t2.clue_see_num,
t2.clue_subscribe_num,
t2.clue_sign_num,

current_timestamp() as etl_time 

from julive_dim.dim_clue_info t1 -- 取线索基本信息 
left join julive_fact.fact_clue_full_line_indi t2 on t1.clue_id = t2.clue_id -- 取线索计算指标类(行为数据)信息 
left join julive_dim.dim_channel_info t3 on t1.channel_id = t3.channel_id -- 取线索渠道信息 
left join julive_dim.dim_employee_info t4 on t1.emp_id = t4.emp_id and t4.end_date = '9999-12-31' -- 取当前咨询师基本信息 及 当前主管信息 
left join julive_dim.dim_ps_employee_info t5 on t1.emp_id = t5.emp_id and regexp_replace(t1.create_date,'-','') = t5.pdate -- 取咨询师业务发生时数据信息 
;

