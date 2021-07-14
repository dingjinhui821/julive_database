
set hive.execution.engine=spark;
set spark.app.name=fact_zxs_adjust_cust_sign_dtl;
set spark.yarn.queue=etl;
insert overwrite table julive_fact.fact_zxs_adjust_cust_sign_base_dtl 

select 

t1.sign_id                                                  as sign_id,
t2.org_id                                                   as org_id,
t2.org_type                                                 as org_type,
t2.org_name                                                 as org_name,
t1.employee_id                                              as emp_id,
t3.employee_name                                            as emp_name,
t1.manager_id                                               as emp_mgr_id,
t4.employee_name                                            as emp_mgr_name,
t1.manager_leader_id                                        as emp_mgr_leader_id,
t8.employee_name                                            as emp_mgr_leader_name, 
t1.city_id                                                  as clue_city_id,
if(t5.city_name is not null,t5.city_name,t10.city_name)     as clue_city_name,
if(t5.city_seq is not null,t5.city_name,t10.city_seq)       as clue_city_seq,
t1.employee_adjust_city                                     as adjust_city_id,
t6.city_name                                                as adjust_city_name, 
t6.city_seq                                                 as adjust_city_seq,
t1.manager_adjust_city                                      as mgr_adjust_city_id,
t7.city_name                                                as mgr_adjust_city_name, 
t7.city_seq                                                 as mgr_adjust_city_seq,
t1.manager_leader_adjust_city                               as mgr_leader_adjust_city_id,
t9.city_name                                                as mgr_leader_adjust_city_name,
t9.city_seq                                                 as mgr_leader_adjust_city_seq,

t2.sign_status                                              as sign_status,
t2.sign_type                                                as sign_type,
t2.orig_sign_income                                         as orig_sign_income,
t1.value                                                    as orig_adjust_sign_num, 
-- 指标:收入 
t2.sign_contains_cancel_ext_income * t1.value               as adjust_sign_contains_cancel_ext_income,  -- 核算签约-含退、含外联收入(佣金)
t2.sign_contains_ext_income * t1.value                      as adjust_sign_contains_ext_income,         -- 核算签约-不含退、含外联收入(佣金)
t2.sign_contains_cancel_income * t1.value                   as adjust_sign_contains_cancel_income,      -- 核算签约-含退、不含外联收入(佣金)
t2.sign_coop_income * t1.value                              as adjust_sign_coop_income,                 -- 核算签约-合作、不含外联收入(佣金)
-- 指标:量 
t2.sign_contains_cancel_ext_num * t1.value                  as adjust_sign_contains_cancel_ext_num,  -- 核算签约量-含退、含外联
t2.sign_contains_ext_num * t1.value                         as adjust_sign_contains_ext_num,         -- 核算签约量-不含退、含外联
t2.sign_contains_cancel_num * t1.value                      as adjust_sign_contains_cancel_num,      -- 核算签约量-含退、不含外联
t2.sign_coop_num * t1.value                                 as adjust_sign_coop_num,                 -- 核算签约量-合作、不含外联

to_date(t2.sign_time)                                       as sign_date,
to_date(from_unixtime(t1.create_datetime))                  as create_date,
to_date(from_unixtime(t1.happen_updatetime))                as happen_date,
t2.from_source                                              as from_source,
current_timestamp()                                         as etl_time 

from ods.adjust_sign_employee_detail t1 
join julive_fact.fact_sign_base_dtl t2 on t1.sign_id = t2.sign_id 

left join ods.yw_employee t3 on t1.employee_id = t3.id  
left join ods.yw_employee t4 on t1.manager_id = t4.id 

left join julive_dim.dim_city t5 on t1.city_id = t5.city_id 
left join julive_dim.dim_city t6 on t1.employee_adjust_city = t6.city_id 
left join julive_dim.dim_city t7 on t1.manager_adjust_city = t7.city_id 

left join ods.yw_employee t8 on t1.manager_leader_id = t8.id 
left join julive_dim.dim_city t9 on t1.manager_leader_adjust_city = t9.city_id
left join julive_dim.dim_wlmq_city t10 on t10.city_id = t1.city_id

where t1.employee_id > 0 
and (t2.audit_status != 2 or t2.audit_status is null)

;

insert overwrite table julive_fact.fact_zxs_adjust_cust_sign_dtl
select* from julive_fact.fact_zxs_adjust_cust_sign_base_dtl
where from_source =1;

insert overwrite table julive_fact.fact_wlmq_zxs_adjust_cust_sign_dtl
select* from julive_fact.fact_zxs_adjust_cust_sign_base_dtl
where from_source =2;
insert overwrite table julive_fact.fact_esf_zxs_adjust_cust_sign_dtl
select* from julive_fact.fact_zxs_adjust_cust_sign_base_dtl
where from_source =3;

insert overwrite table julive_fact.fact_jms_zxs_adjust_cust_sign_dtl
select* from julive_fact.fact_zxs_adjust_cust_sign_base_dtl
where from_source =4;

insert into table julive_fact.fact_jms_zxs_adjust_cust_sign_dtl
select* from julive_fact.fact_zxs_adjust_cust_sign_base_dtl
where from_source =1 and org_id!=48;


