

-- ----------------------------------------------------------------------------------------------------------------------
-- ETL scripts 


set hive.execution.engine=spark;
set spark.app.name=fact_city_day_agg_indi;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;

with fact_clue_city_day_indi as ( -- 线索日-城市指标 

select 

t2.date_str                                                               as date_str,
t2.date_str_zh                                                            as date_str_zh,
t1.org_id                                                                 as org_id,    
t1.org_type                                                               as org_type,
t1.org_name                                                               as org_name,
t1.customer_intent_city_id                                                as city_id,
t1.customer_intent_city_name                                              as city_name,
t1.customer_intent_city_seq                                               as city_seq,

count(1)                                                                  as clue_num,
t1.from_source                                                            as from_source,
current_timestamp()                                                       as etl_time

from julive_dim.dim_clue_base_info t1 
left join julive_dim.dim_date t2 on regexp_replace(t1.create_date,"-","") = t2.skey 
where t1.create_date >= '2017-01-01' 
and t1.create_date <= current_date() 

group by 
t2.date_str,
t2.date_str_zh,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.customer_intent_city_id,
t1.customer_intent_city_name,
t1.customer_intent_city_seq,
t1.from_source 
),
fact_distribute_city_day_indi as ( -- 上户日-城市指标 

select 

t2.date_str                                                               as date_str,
t2.date_str_zh                                                            as date_str_zh,
t1.org_id                                                                 as org_id,    
t1.org_type                                                               as org_type,
t1.org_name                                                               as org_name,
t1.customer_intent_city_id                                                as city_id,
t1.customer_intent_city_name                                              as city_name,
t1.customer_intent_city_seq                                               as city_seq,

sum(if(t1.is_distribute = 1,1,0))                                         as distribute_num,
sum(distinct if(t1.is_distribute = 1,to_date(t1.distribute_time),null))   as distribute_day_num,
t1.from_source                                                            as from_source,
current_timestamp()                                                       as etl_time

from julive_dim.dim_clue_base_info t1 
left join julive_dim.dim_date t2 on regexp_replace(to_date(t1.distribute_time),"-","") = t2.skey 
where to_date(t1.distribute_time) >= '2017-01-01' 
and to_date(t1.distribute_time) <= current_date() 
and t1.is_distribute = 1 

group by 
t2.date_str,
t2.date_str_zh,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.customer_intent_city_id,
t1.customer_intent_city_name,
t1.customer_intent_city_seq,
t1.from_source  
),
fact_see_project_city_day_indi as ( -- 带看日-城市指标 
select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.project_city_id                             as city_id,
t1.project_city_name                           as city_name,
t1.project_city_seq                            as city_seq,

sum(t1.see_num)                                as see_num,
sum(t1.see_project_num)                        as see_project_num,
t1.from_source                                 as from_source,
current_timestamp()                            as etl_time

from julive_fact.fact_see_project_base_dtl t1 
join julive_dim.dim_date t2 on regexp_replace(to_date(t1.plan_real_begin_time),"-","") = t2.skey 
where to_date(t1.plan_real_begin_time) <= current_date() 

group by 
t2.date_str,
t2.date_str_zh,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.project_city_id,
t1.project_city_name,
t1.project_city_seq,
t1.from_source  
),
fact_subscribe_city_day_indi as ( -- 计算认购日-城市指标表

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.project_city_id                             as city_id,
t1.project_city_name                           as city_name,
t1.project_city_seq                            as city_seq,

sum(t1.subscribe_contains_cancel_ext_num)      as subscribe_contains_cancel_ext_num,
sum(t1.subscribe_contains_cancel_ext_amt)      as subscribe_contains_cancel_ext_amt,
sum(t1.subscribe_contains_cancel_ext_income)   as subscribe_contains_cancel_ext_income,
count(distinct t1.project_id)                  as subscribe_contains_cancel_ext_project_num,
sum(t1.subscribe_contains_ext_amt)             as subscribe_contains_ext_amt,
sum(t1.subscribe_contains_ext_income)          as subscribe_contains_ext_income,
sum(t1.subscribe_contains_ext_num)             as subscribe_contains_ext_num,
sum(t1.subscribe_coop_num)                     as subscribe_coop_num,
t1.from_source                                 as from_source,
current_timestamp()                            as etl_time

from julive_fact.fact_subscribe_base_dtl t1 
join julive_dim.dim_date t2 on regexp_replace(to_date(t1.subscribe_time),"-","") = t2.skey 
where to_date(t1.subscribe_time) <= current_date() 

group by 
t2.date_str,
t2.date_str_zh,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.project_city_id,
t1.project_city_name,
t1.project_city_seq,
t1.from_source  
),
fact_back_subscribe_city_day_indi as ( -- 计算退认购 日-城市快照指标

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.project_city_id                             as city_id,
t1.project_city_name                           as city_name,
t1.project_city_seq                            as city_seq,

sum(t1.subscribe_cancel_contains_ext_amt)      as subscribe_cancel_contains_ext_amt,
sum(t1.subscribe_cancel_contains_ext_num)      as subscribe_cancel_contains_ext_num,
sum(t1.subscribe_cancel_contains_ext_income)   as subscribe_cancel_contains_ext_income,
t1.from_source                                 as from_source,
current_timestamp()                            as etl_time

from julive_fact.fact_subscribe_base_dtl t1 
join julive_dim.dim_date t2 on regexp_replace(to_date(t1.back_time),"-","") = t2.skey 
where to_date(t1.back_time) <= current_date() 

group by 
t2.date_str,
t2.date_str_zh,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.project_city_id,
t1.project_city_name,
t1.project_city_seq,
t1.from_source  
),
fact_sign_city_day_indi as ( -- 签约日-城市指标表

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.project_city_id                             as city_id,
t1.project_city_name                           as city_name,
t1.project_city_seq                            as city_seq,

sum(t1.sign_contains_cancel_ext_num)           as sign_contains_cancel_ext_num,
sum(t1.sign_contains_cancel_ext_income)        as sign_contains_cancel_ext_income,
sum(t1.sign_contains_ext_income)               as sign_contains_ext_income, -- 20191210添加 
sum(t1.sign_contains_ext_num)                  as sign_contains_ext_num,
sum(t1.sign_cancel_contains_ext_num)           as sign_cancel_contains_ext_num,
sum(t1.sign_coop_num)                          as sign_coop_num,
t1.from_source                                 as from_source,
current_timestamp()                            as etl_time

from julive_fact.fact_sign_base_dtl t1 
join julive_dim.dim_date t2 on regexp_replace(to_date(t1.sign_time),"-","") = t2.skey 
where to_date(t1.sign_time) <= current_date()
and t1.audit_status != 2 

group by 
t2.date_str,
t2.date_str_zh,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.project_city_id,
t1.project_city_name,
t1.project_city_seq,
t1.from_source  
),
fact_see_project_emp_city_day_indi as ( -- 带看日-城市指标 
select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.emp_city_id                                 as emp_city_id,
t1.emp_city_name                               as emp_city_name,
t1.emp_city_seq                                as emp_city_seq,

sum(t1.see_num)                                as emp_city_see_num,
sum(t1.see_project_num)                        as emp_city_see_project_num,
t1.from_source                                 as from_source,
current_timestamp()                            as etl_time

from julive_fact.fact_see_project_base_dtl t1 
join julive_dim.dim_date t2 on regexp_replace(to_date(t1.plan_real_begin_time),"-","") = t2.skey 
where to_date(t1.plan_real_begin_time) <= current_date() 

group by 
t2.date_str,
t2.date_str_zh,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.emp_city_id,
t1.emp_city_name,
t1.emp_city_seq,
t1.from_source  
),
fact_subscribe_emp_city_day_indi as ( -- 计算认购日-城市指标表

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.emp_city_id                                 as emp_city_id,
t1.emp_city_name                               as emp_city_name,
t1.emp_city_seq                                as emp_city_seq,

sum(t1.subscribe_contains_cancel_ext_num)      as emp_city_subscribe_contains_cancel_ext_num,
sum(t1.subscribe_contains_cancel_ext_amt)      as emp_city_subscribe_contains_cancel_ext_amt,
sum(t1.subscribe_contains_cancel_ext_income)   as emp_city_subscribe_contains_cancel_ext_income,
count(distinct t1.project_id)                  as emp_city_subscribe_contains_cancel_ext_project_num,
sum(t1.subscribe_contains_ext_amt)             as emp_city_subscribe_contains_ext_amt,
sum(t1.subscribe_contains_ext_income)          as emp_city_subscribe_contains_ext_income,
sum(t1.subscribe_contains_ext_num)             as emp_city_subscribe_contains_ext_num,
sum(t1.subscribe_coop_num)                     as emp_city_subscribe_coop_num,
t1.from_source                                 as from_source,
current_timestamp()                            as etl_time

from julive_fact.fact_subscribe_base_dtl t1 
join julive_dim.dim_date t2 on regexp_replace(to_date(t1.subscribe_time),"-","") = t2.skey 
where to_date(t1.subscribe_time) <= current_date() 

group by 
t2.date_str,
t2.date_str_zh,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.emp_city_id,
t1.emp_city_name,
t1.emp_city_seq,
t1.from_source  
),
fact_back_subscribe_emp_city_day_indi as ( -- 计算退认购 日-城市快照指标

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.emp_city_id                                 as emp_city_id,
t1.emp_city_name                               as emp_city_name,
t1.emp_city_seq                                as emp_city_seq,

sum(t1.subscribe_cancel_contains_ext_amt)      as emp_city_subscribe_cancel_contains_ext_amt,
sum(t1.subscribe_cancel_contains_ext_num)      as emp_city_subscribe_cancel_contains_ext_num,
sum(t1.subscribe_cancel_contains_ext_income)   as emp_city_subscribe_cancel_contains_ext_income,
t1.from_source                                 as from_source,
current_timestamp()                            as etl_time

from julive_fact.fact_subscribe_base_dtl t1 
join julive_dim.dim_date t2 on regexp_replace(to_date(t1.back_time),"-","") = t2.skey 
where to_date(t1.back_time) <= current_date() 

group by 
t2.date_str,
t2.date_str_zh,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.emp_city_id,
t1.emp_city_name,
t1.emp_city_seq,
t1.from_source  
),
fact_sign_emp_city_day_indi as ( -- 签约日-城市指标表

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.emp_city_id                                 as emp_city_id,
t1.emp_city_name                               as emp_city_name,
t1.emp_city_seq                                as emp_city_seq,

sum(t1.sign_contains_cancel_ext_num)           as emp_city_sign_contains_cancel_ext_num,
sum(t1.sign_contains_cancel_ext_income)        as emp_city_sign_contains_cancel_ext_income,
sum(t1.sign_contains_ext_income)               as emp_city_sign_contains_ext_income, -- 20191210添加 
sum(t1.sign_contains_ext_num)                  as emp_city_sign_contains_ext_num,
sum(t1.sign_cancel_contains_ext_num)           as emp_city_sign_cancel_contains_ext_num,
sum(t1.sign_coop_num)                          as emp_city_sign_coop_num,
t1.from_source                                 as from_source,
current_timestamp()                            as etl_time

from julive_fact.fact_sign_base_dtl t1 
join julive_dim.dim_date t2 on regexp_replace(to_date(t1.sign_time),"-","") = t2.skey 
where to_date(t1.sign_time) <= current_date()
and t1.audit_status != 2 

group by 
t2.date_str,
t2.date_str_zh,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.emp_city_id,
t1.emp_city_name,
t1.emp_city_seq,
t1.from_source  
),
fact_payment_city_day_indi as ( -- 回款日-城市指标表

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t1.city_id                                     as city_id,
t1.city_name                                   as city_name,
t1.city_seq                                    as city_seq,
t1.from_source                                 as from_source,

sum(t1.actual_amt)                             as actual_amt,
current_timestamp()                            as etl_time

from julive_fact.fact_payment_base_dtl t1 
join julive_dim.dim_date t2 on regexp_replace(to_date(t1.actual_time),"-","") = t2.skey 
where to_date(t1.actual_time) <= current_date() 

group by 
t2.date_str,
t2.date_str_zh,
t1.city_id,
t1.city_name,
t1.city_seq,
t1.from_source  
),
fact_emp_check_city_day_indi as ( -- 员工出勤日-城市指标表 
select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t1.city_id                                     as city_id,
t1.city_name                                   as city_name,
t1.city_seq                                    as city_seq,
t1.from_source                                 as from_source,

sum(t1.real_workday_num)                       as real_workday_num,
current_timestamp()                            as etl_time

from julive_fact.fact_employee_check_base_dtl t1 
join julive_dim.dim_date t2 on regexp_replace(t1.check_date,"-","") = t2.skey 
where t1.check_date <= current_date() 

group by 
t2.date_str,
t2.date_str_zh,
t1.city_id,
t1.city_name,
t1.city_seq,
t1.from_source  
) 

-- 城市-天指标表 
insert overwrite table julive_fact.fact_city_day_agg_base_indi 

select 

coalesce(t1.date_str,t2.date_str,t3.date_str,t4.date_str,t5.date_str,t6.date_str,t7.date_str,t8.date_str,t10.date_str,t11.date_str,t12.date_str,t13.date_str)                      
     as date_str,
coalesce(t1.date_str_zh,t2.date_str_zh,t3.date_str_zh,t4.date_str_zh,t5.date_str_zh,t6.date_str_zh,t7.date_str_zh,t8.date_str_zh,t10.date_str_zh,t11.date_str_zh,t12.date_str_zh,t13.date_str_zh)
     as date_str_zh,
coalesce(t1.org_id,t2.org_id,t3.org_id,t4.org_id,t6.org_id,t8.org_id,t10.org_id,t11.org_id,t12.org_id,t12.org_id)                                                                    
     as org_id,  
coalesce(t1.org_type,t2.org_type,t3.org_type,t4.org_type,t6.org_type,t8.org_type,t10.org_type,t11.org_type,t12.org_type,t13.org_type)                                            
     as org_type,
coalesce(t1.org_name,t2.org_name,t3.org_name,t4.org_name,t6.org_name,t8.org_name,t10.org_name,t11.org_name,t12.org_name,t13.org_name)                                             
     as org_name,
coalesce(t1.city_id,t2.city_id,t3.city_id,t4.city_id,t5.city_id,t6.city_id,t7.city_id,t8.city_id)                             
     as city_id,
coalesce(t1.city_name,t2.city_name,t3.city_name,t4.city_name,t5.city_name,t6.city_name,t7.city_name,t8.city_name)              
     as city_name,
coalesce(t1.city_seq,t2.city_seq,t3.city_seq,t4.city_seq,t5.city_seq,t6.city_seq,t7.city_seq,t8.city_seq)                     
     as city_seq,
coalesce(t10.emp_city_id,t11.emp_city_id,t12.emp_city_id,t13.emp_city_id)
     as emp_city_id,
coalesce(t10.emp_city_name,t11.emp_city_name,t12.emp_city_name,t13.emp_city_name)
     as emp_city_name,
coalesce(t10.emp_city_seq,t11.emp_city_seq,t12.emp_city_seq,t13.emp_city_seq)
     as emp_city_seq,
t9.region  as city_region,
t9.city_type as city_type,
t9.mgr_city, --2019-12-30增加

t1.clue_num,
t8.distribute_num,
t8.distribute_day_num,

t2.see_num,
t2.see_project_num,
t10.emp_city_see_num,
t10.emp_city_see_project_num,

t3.subscribe_contains_cancel_ext_num,
t3.subscribe_contains_cancel_ext_amt,
t3.subscribe_contains_cancel_ext_income,
t3.subscribe_contains_cancel_ext_project_num,
t3.subscribe_contains_ext_amt,
t3.subscribe_contains_ext_income,
t11.emp_city_subscribe_contains_cancel_ext_num,
t11.emp_city_subscribe_contains_cancel_ext_amt,
t11.emp_city_subscribe_contains_cancel_ext_income,
t11.emp_city_subscribe_contains_cancel_ext_project_num,
t11.emp_city_subscribe_contains_ext_amt,
t11.emp_city_subscribe_contains_ext_income,

t6.subscribe_cancel_contains_ext_amt,
t13.emp_city_subscribe_cancel_contains_ext_amt,
t3.subscribe_contains_ext_num,
t11.emp_city_subscribe_contains_ext_num,
t6.subscribe_cancel_contains_ext_num,
t6.subscribe_cancel_contains_ext_income,
t13.emp_city_subscribe_cancel_contains_ext_num,
t13.emp_city_subscribe_cancel_contains_ext_income,
t3.subscribe_coop_num,
t11.emp_city_subscribe_coop_num,

t4.sign_contains_cancel_ext_num,
t4.sign_contains_cancel_ext_income,
t4.sign_contains_ext_income,
t4.sign_contains_ext_num,
t4.sign_cancel_contains_ext_num,
t4.sign_coop_num,
t12.emp_city_sign_contains_cancel_ext_num,
t12.emp_city_sign_contains_cancel_ext_income,
t12.emp_city_sign_contains_ext_income,
t12.emp_city_sign_contains_ext_num,
t12.emp_city_sign_cancel_contains_ext_num,
t12.emp_city_sign_coop_num,

t5.actual_amt,
t7.real_workday_num,
coalesce(t1.from_source,t2.from_source,t3.from_source,t4.from_source,t5.from_source,t6.from_source,t7.from_source,t8.from_source,t10.from_source,t11.from_source,t12.from_source,t13.from_source)  as from_source,

current_timestamp() as etl_time 

from fact_clue_city_day_indi t1 -- 线索 
full join fact_see_project_city_day_indi t2 on t1.date_str = t2.date_str and t1.city_id = t2.city_id and t1.from_source = t2.from_source and t1.org_id= t2.org_id
full join fact_subscribe_city_day_indi t3 on t1.date_str = t3.date_str and t1.city_id = t3.city_id and t1.from_source = t3.from_source and t1.org_id= t3.org_id
full join fact_sign_city_day_indi t4 on t1.date_str = t4.date_str and t1.city_id = t4.city_id and t1.from_source = t4.from_source and t1.org_id= t4.org_id
full join fact_payment_city_day_indi t5 on t1.date_str = t5.date_str and t1.city_id = t5.city_id and t1.from_source = t5.from_source 
full join fact_back_subscribe_city_day_indi t6 on t1.date_str = t6.date_str and t1.city_id = t6.city_id and t1.from_source = t6.from_source and t1.org_id= t6.org_id
full join fact_emp_check_city_day_indi t7 on t1.date_str = t7.date_str and t1.city_id = t7.city_id and t1.from_source = t7.from_source
full join fact_distribute_city_day_indi t8 on t1.date_str = t8.date_str and t1.city_id = t8.city_id and t1.from_source = t8.from_source and t1.org_id= t8.org_id


full join fact_see_project_emp_city_day_indi t10 on t1.date_str = t10.date_str and t1.city_id = t10.emp_city_id and t1.from_source = t10.from_source and t1.org_id= t10.org_id
full join fact_subscribe_emp_city_day_indi t11 on t1.date_str = t11.date_str and t1.city_id = t11.emp_city_id and t1.from_source = t11.from_source and t1.org_id= t11.org_id
full join fact_sign_emp_city_day_indi t12 on t1.date_str = t12.date_str and t1.city_id = t12.emp_city_id and t1.from_source = t12.from_source and t1.org_id= t12.org_id
full join fact_back_subscribe_emp_city_day_indi t13 on t1.date_str = t13.date_str and t1.city_id = t13.emp_city_id and t1.from_source = t13.from_source and t1.org_id= t13.org_id
left join julive_dim.dim_city t9 on  coalesce(t1.city_id,t2.city_id,t3.city_id,t4.city_id,t5.city_id,t6.city_id,t7.city_id,t8.city_id,t10.emp_city_id,t11.emp_city_id,t12.emp_city_id,t13.emp_city_id)=t9.city_id
;

insert overwrite table julive_fact.fact_city_day_agg_indi
select * from julive_fact.fact_city_day_agg_base_indi
where from_source = 1;

insert overwrite table julive_fact.fact_wlmq_city_day_agg_indi
select * from julive_fact.fact_city_day_agg_base_indi
where from_source = 2;

insert overwrite table julive_fact.fact_esf_city_day_agg_indi
select * from julive_fact.fact_city_day_agg_base_indi
where from_source = 3;

insert overwrite table julive_fact.fact_jms_city_day_agg_indi
select * from julive_fact.fact_city_day_agg_base_indi
where from_source = 4;

insert into table julive_fact.fact_jms_city_day_agg_indi
select * from julive_fact.fact_city_day_agg_base_indi
where from_source = 1 and org_id!=48;


