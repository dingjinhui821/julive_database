
set hive.execution.engine=spark;
set spark.app.name=fact_mgr_adjust_perf_indi;
set spark.yarn.queue=etl;
with tmp_adjust_distribute as ( -- 上户核算 
select 

emp_mgr_id                          as emp_mgr_id,
mgr_adjust_city_id                  as mgr_adjust_city_id,
create_date                         as happen_date,

sum(adjust_distribute_num)          as adjust_distribute_num,
sum(first_adjust_distribute_num)    as first_adjust_distribute_num

from julive_fact.fact_zxs_adjust_cust_distribute_dtl 
group by 
emp_mgr_id,
mgr_adjust_city_id,
create_date
),
tmp_adjust_see as ( -- 带看核算 
select 

emp_mgr_id                          as emp_mgr_id,
mgr_adjust_city_id                  as mgr_adjust_city_id,
plan_real_begin_date                as happen_date,
sum(adjust_see_num)                 as adjust_see_num

from julive_fact.fact_zxs_adjust_cust_see_dtl 
group by 
emp_mgr_id,
mgr_adjust_city_id,
plan_real_begin_date
),
tmp_adjust_subscribe as ( -- 认购核算 
select 

emp_mgr_id                                       as emp_mgr_id,
mgr_adjust_city_id                               as mgr_adjust_city_id,
subscribe_date                                   as happen_date,

sum(adjust_subscribe_contains_cancel_ext_income) as adjust_subscribe_contains_cancel_ext_income,
sum(adjust_subscribe_contains_ext_income)        as adjust_subscribe_contains_ext_income,
sum(adjust_subscribe_contains_cancel_income)     as adjust_subscribe_contains_cancel_income,
sum(adjust_subscribe_coop_income)                as adjust_subscribe_coop_income,
sum(adjust_subscribe_contains_cancel_ext_num)    as adjust_subscribe_contains_cancel_ext_num,
sum(adjust_subscribe_contains_ext_num)           as adjust_subscribe_contains_ext_num,
sum(adjust_subscribe_contains_cancel_num)        as adjust_subscribe_contains_cancel_num,
sum(adjust_subscribe_coop_num)                   as adjust_subscribe_coop_num 

from julive_fact.fact_zxs_adjust_cust_subscribe_dtl 
group by 
emp_mgr_id,
mgr_adjust_city_id,
subscribe_date
),
tmp_adjust_subscribe_cancel as ( -- 退认购核算 
select 

emp_mgr_id                                       as emp_mgr_id,
mgr_adjust_city_id                               as mgr_adjust_city_id,
back_date                                        as happen_date,

sum(adjust_subscribe_cancel_contains_ext_income) as adjust_subscribe_cancel_contains_ext_income,
sum(adjust_subscribe_cancel_contains_ext_num)    as adjust_subscribe_cancel_contains_ext_num

from julive_fact.fact_zxs_adjust_cust_subscribe_cancel_dtl 
group by 
emp_mgr_id,
mgr_adjust_city_id,
back_date
),
tmp_adjust_sign as ( -- 签约核算 
select 

emp_mgr_id                                  as emp_mgr_id,
mgr_adjust_city_id                          as mgr_adjust_city_id,
sign_date                                   as happen_date,
sum(adjust_sign_contains_cancel_ext_income) as adjust_sign_contains_cancel_ext_income,
sum(adjust_sign_contains_ext_income)        as adjust_sign_contains_ext_income,
sum(adjust_sign_contains_cancel_income)     as adjust_sign_contains_cancel_income,
sum(adjust_sign_coop_income)                as adjust_sign_coop_income,
sum(adjust_sign_contains_cancel_ext_num)    as adjust_sign_contains_cancel_ext_num,
sum(adjust_sign_contains_ext_num)           as adjust_sign_contains_ext_num,
sum(adjust_sign_contains_cancel_num)        as adjust_sign_contains_cancel_num,
sum(adjust_sign_coop_num)                   as adjust_sign_coop_num 

from julive_fact.fact_zxs_adjust_cust_sign_dtl 
group by 
emp_mgr_id,
mgr_adjust_city_id,
sign_date
),
tmp_adjust_result as ( -- 核算指标 
select 

coalesce(t1.emp_mgr_id,t2.emp_mgr_id,t3.emp_mgr_id,t4.emp_mgr_id,t5.emp_mgr_id)                                         as emp_mgr_id,
coalesce(t1.mgr_adjust_city_id,t2.mgr_adjust_city_id,t3.mgr_adjust_city_id,t4.mgr_adjust_city_id,t5.mgr_adjust_city_id) as mgr_adjust_city_id,
coalesce(t1.happen_date,t2.happen_date,t3.happen_date,t4.happen_date,t5.happen_date)                                    as happen_date,

t1.adjust_distribute_num,
t1.first_adjust_distribute_num,
t2.adjust_see_num,
-- 指标:佣金 
t3.adjust_subscribe_contains_cancel_ext_income,
t3.adjust_subscribe_contains_ext_income,
t3.adjust_subscribe_contains_cancel_income,
t3.adjust_subscribe_coop_income,
t3.adjust_subscribe_contains_cancel_ext_num,
t3.adjust_subscribe_contains_ext_num,
t3.adjust_subscribe_contains_cancel_num,
t3.adjust_subscribe_coop_num,
-- 指标：退认购 
t4.adjust_subscribe_cancel_contains_ext_income,
t4.adjust_subscribe_cancel_contains_ext_num,
-- 指标:佣金 
t5.adjust_sign_contains_cancel_ext_income,
t5.adjust_sign_contains_ext_income,
t5.adjust_sign_contains_cancel_income,
t5.adjust_sign_coop_income,
t5.adjust_sign_contains_cancel_ext_num,
t5.adjust_sign_contains_ext_num,
t5.adjust_sign_contains_cancel_num,
t5.adjust_sign_coop_num

from tmp_adjust_distribute            t1
full join tmp_adjust_see              t2 on t1.emp_mgr_id = t2.emp_mgr_id and t1.happen_date = t2.happen_date and t1.mgr_adjust_city_id = t2.mgr_adjust_city_id 
full join tmp_adjust_subscribe        t3 on t1.emp_mgr_id = t3.emp_mgr_id and t1.happen_date = t3.happen_date and t1.mgr_adjust_city_id = t3.mgr_adjust_city_id 
full join tmp_adjust_subscribe_cancel t4 on t1.emp_mgr_id = t4.emp_mgr_id and t1.happen_date = t4.happen_date and t1.mgr_adjust_city_id = t4.mgr_adjust_city_id 
full join tmp_adjust_sign             t5 on t1.emp_mgr_id = t5.emp_mgr_id and t1.happen_date = t5.happen_date and t1.mgr_adjust_city_id = t5.mgr_adjust_city_id 
) 

insert overwrite table julive_fact.fact_mgr_adjust_perf_indi 

select 

coalesce(t1.emp_id,t2.emp_mgr_id) as emp_mgr_id,
t1.emp_name                   as emp_mgr_name,
t2.mgr_adjust_city_id         as mgr_adjust_city_id,
t4.city_name                  as mgr_adjust_city_name,
t4.city_seq                   as mgr_adjust_city_seq,

t1.adjust_city_id             as zxs_mgr_adjust_city_id,
t1.adjust_city_name           as zxs_mgr_adjust_city_name,
t5.city_seq                   as zxs_mgr_adjust_city_seq,

t2.happen_date,

t1.entry_date,
t1.full_date,
t1.full_type,
t1.offjob_date,
t1.post_id,
t1.post_name,
t1.dept_id,
t1.dept_name,
t1.direct_leader_id,
t1.direct_leader_name,
t1.indirect_leader_id,
t1.indirect_leader_name,
t3.direct_leader_id       as now_direct_leader_id,
t3.direct_leader_name     as now_direct_leader_name,
t3.indirect_leader_id     as now_indirect_leader_id,
t3.indirect_leader_name   as now_indirect_leader_name,
''                        as promotion_date,
-- 指标 
coalesce(t2.adjust_distribute_num,0),
coalesce(t2.first_adjust_distribute_num,0),
coalesce(t2.adjust_see_num,0),
coalesce(t2.adjust_subscribe_contains_cancel_ext_income,0),
coalesce(t2.adjust_subscribe_contains_ext_income,0),
coalesce(t2.adjust_subscribe_contains_cancel_income,0),
coalesce(t2.adjust_subscribe_coop_income,0),
coalesce(t2.adjust_subscribe_contains_cancel_ext_num,0),
coalesce(t2.adjust_subscribe_contains_ext_num,0),
coalesce(t2.adjust_subscribe_contains_cancel_num,0),
coalesce(t2.adjust_subscribe_coop_num,0),
coalesce(t2.adjust_subscribe_cancel_contains_ext_income,0),
coalesce(t2.adjust_subscribe_cancel_contains_ext_num,0),
coalesce(t2.adjust_sign_contains_cancel_ext_income,0),
coalesce(t2.adjust_sign_contains_ext_income,0),
coalesce(t2.adjust_sign_contains_cancel_income,0),
coalesce(t2.adjust_sign_coop_income,0),
coalesce(t2.adjust_sign_contains_cancel_ext_num,0),
coalesce(t2.adjust_sign_contains_ext_num,0),
coalesce(t2.adjust_sign_contains_cancel_num,0),
coalesce(t2.adjust_sign_coop_num,0),
current_timestamp() as etl_time 

from julive_dim.dim_consultant_info t1 
full join tmp_adjust_result t2 on t1.emp_id = t2.emp_mgr_id and t1.pdate = regexp_replace(t2.happen_date,'-','') 

left join julive_dim.dim_employee_info t3 on t1.emp_id = t3.emp_id and t3.end_date = '9999-12-31' 
left join julive_dim.dim_city t4 on t2.mgr_adjust_city_id = t4.city_id 
left join julive_dim.dim_city t5 on t1.adjust_city_id = t5.city_id 

where t1.job_status = 1 -- 在职 
;

