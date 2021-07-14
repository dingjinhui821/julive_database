
set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=fact_wlmq_zxs_adjust_perf_indi;
set spark.yarn.queue=etl;

with tmp_adjust_distribute as ( -- 上户核算 
select

emp_id                              as emp_id,
adjust_city_id                      as adjust_city_id,
create_date                         as happen_date,

sum(adjust_distribute_num)          as adjust_distribute_num,
sum(first_adjust_distribute_num)    as first_adjust_distribute_num

from julive_fact.fact_wlmq_zxs_adjust_cust_distribute_dtl
group by
emp_id,
adjust_city_id,
create_date
),
tmp_adjust_see as ( -- 带看核算 
select

emp_id                         as emp_id,
adjust_city_id                 as adjust_city_id,
plan_real_begin_date           as happen_date,

sum(adjust_see_num)            as adjust_see_num

from julive_fact.fact_wlmq_zxs_adjust_cust_see_dtl
group by
emp_id,
adjust_city_id,
plan_real_begin_date
),
tmp_adjust_subscribe as ( -- 认购核算 
select

emp_id                                           as emp_id,
adjust_city_id                                   as adjust_city_id,
subscribe_date                                   as happen_date,

sum(adjust_subscribe_contains_cancel_ext_income) as adjust_subscribe_contains_cancel_ext_income,
sum(adjust_subscribe_contains_ext_income)        as adjust_subscribe_contains_ext_income,
sum(adjust_subscribe_contains_cancel_income)     as adjust_subscribe_contains_cancel_income,
sum(adjust_subscribe_coop_income)                as adjust_subscribe_coop_income,
sum(adjust_subscribe_contains_cancel_ext_num)    as adjust_subscribe_contains_cancel_ext_num,
sum(adjust_subscribe_contains_ext_num)           as adjust_subscribe_contains_ext_num,
sum(adjust_subscribe_contains_cancel_num)        as adjust_subscribe_contains_cancel_num,
sum(adjust_subscribe_coop_num)                   as adjust_subscribe_coop_num

from julive_fact.fact_wlmq_zxs_adjust_cust_subscribe_dtl
group by
emp_id,
adjust_city_id,
subscribe_date
),
tmp_adjust_subscribe_cancel as ( -- 退认购核算 
select

emp_id                                           as emp_id,
adjust_city_id                                   as adjust_city_id,
back_date                                        as happen_date,

sum(adjust_subscribe_cancel_contains_ext_income) as adjust_subscribe_cancel_contains_ext_income,
sum(adjust_subscribe_cancel_contains_ext_num)    as adjust_subscribe_cancel_contains_ext_num

from julive_fact.fact_wlmq_zxs_adjust_cust_subscribe_cancel_dtl
group by
emp_id,
adjust_city_id,
back_date
),
tmp_adjust_sign as ( -- 签约核算 
select

emp_id                                      as emp_id,
adjust_city_id                              as adjust_city_id,
sign_date                                   as happen_date,

sum(adjust_sign_contains_cancel_ext_income) as adjust_sign_contains_cancel_ext_income,
sum(adjust_sign_contains_ext_income)        as adjust_sign_contains_ext_income,
sum(adjust_sign_contains_cancel_income)     as adjust_sign_contains_cancel_income,
sum(adjust_sign_coop_income)                as adjust_sign_coop_income,
sum(adjust_sign_contains_cancel_ext_num)    as adjust_sign_contains_cancel_ext_num,
sum(adjust_sign_contains_ext_num)           as adjust_sign_contains_ext_num,
sum(adjust_sign_contains_cancel_num)        as adjust_sign_contains_cancel_num,
sum(adjust_sign_coop_num)                   as adjust_sign_coop_num

from julive_fact.fact_wlmq_zxs_adjust_cust_sign_dtl
group by
emp_id,
adjust_city_id,
sign_date
),
tmp_adjust_result as ( -- 核算指标 
select

coalesce(t1.emp_id,t2.emp_id,t3.emp_id,t4.emp_id,t5.emp_id)                                         as emp_id,
coalesce(t1.adjust_city_id,t2.adjust_city_id,t3.adjust_city_id,t4.adjust_city_id,t5.adjust_city_id) as adjust_city_id,
coalesce(t1.happen_date,t2.happen_date,t3.happen_date,t4.happen_date,t5.happen_date)                as happen_date,

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
full join tmp_adjust_see              t2 on t1.emp_id = t2.emp_id and t1.happen_date = t2.happen_date and t1.adjust_city_id = t2.adjust_city_id
full join tmp_adjust_subscribe        t3 on t1.emp_id = t3.emp_id and t1.happen_date = t3.happen_date and t1.adjust_city_id = t3.adjust_city_id
full join tmp_adjust_subscribe_cancel t4 on t1.emp_id = t4.emp_id and t1.happen_date = t4.happen_date and t1.adjust_city_id = t4.adjust_city_id
full join tmp_adjust_sign             t5 on t1.emp_id = t5.emp_id and t1.happen_date = t5.happen_date and t1.adjust_city_id = t5.adjust_city_id
)


insert overwrite table julive_fact.fact_wlmq_zxs_adjust_perf_indi

select

coalesce(t1.emp_id,t2.emp_id)                      as emp_id,
t6.employee_name                                   as emp_name,
coalesce(t2.adjust_city_id,t1.adjust_city_id)      as adjust_city_id,
t4.city_name                                       as adjust_city_name,
t4.city_seq                                        as adjust_city_seq,
t4.region                                          as city_region,
t4.city_type_first                                 as city_type,
t4.mgr_city                                        as mgr_city, -----2019-12-30增加

coalesce(t1.adjust_city_id,null)                   as zxs_adjust_city_id,
t5.city_name                                       as zxs_adjust_city_name,
t5.city_seq                                        as zxs_adjust_city_seq,
coalesce(t2.happen_date,t1.create_date)            as happen_date,

t7.entry_date,
t7.full_date,
t7.full_type,
t7.offjob_date,
t7.post_id,
t7.post_name,
t7.dept_id,
t7.dept_name,

t7.direct_leader_id                                 as direct_leader_id,                             
t7.direct_leader_name                               as direct_leader_name,
t7.indirect_leader_id                               as indirect_leader_id,
t7.indirect_leader_name                             as indirect_leader_name,

t3.direct_leader_id                                 as now_direct_leader_id,
t3.direct_leader_name                               as now_direct_leader_name,
t3.indirect_leader_id                               as now_indirect_leader_id,
t3.indirect_leader_name                             as now_indirect_leader_name,
''                                                  as promotion_date,

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

from (
select *
from 
julive_dim.dim_wlmq_employee_info
where pdate =${hiveconf:etl_date}
) t1
full join tmp_adjust_result t2 on t1.emp_id = t2.emp_id and t1.pdate = regexp_replace(t2.happen_date,'-','') and t1.adjust_city_id = t2.adjust_city_id

left join julive_dim.dim_wlmq_employee_info t3 on coalesce(t1.emp_id,t2.emp_id) = t3.emp_id and t3.end_date = from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd')
left join julive_dim.dim_wlmq_city t4 on coalesce(t2.adjust_city_id,t1.adjust_city_id) = t4.city_id
left join julive_dim.dim_wlmq_city t5 on t1.adjust_city_id = t5.city_id
left join ods.yw_employee t6 on coalesce(t1.emp_id,t2.emp_id) = t6.id
left join 
 (select a1.*,a2.date_str
 from julive_dim.dim_wlmq_employee_info a1
  left join julive_dim.dim_date a2
  on from_unixtime(unix_timestamp(a1.pdate,'yyyyMMdd'),'yyyy-MM-dd')=a2.date_id 
  ) t7 
   on coalesce(t1.emp_id,t2.emp_id) = t7.emp_id 
   and coalesce(t2.happen_date,t1.create_date)=t7.date_str
;


