

-- ----------------------------------------------------------------------------------------------------------------------
-- ETL scripts 
set hive.execution.engine=spark;
set spark.app.name=fact_city_channel_source_day_agg_base_indi;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=4g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;


with tmp_clue_city_channel_source_day as (

select 

t2.date_str                                                               as date_str,
t2.date_str_zh                                                            as date_str_zh,
t2.week_type                                                              as week_type,
t1.org_id                                                                 as org_id,    
t1.org_type                                                               as org_type,
t1.org_name                                                               as org_name,
t1.customer_intent_city_id                                                as city_id,
t1.customer_intent_city_name                                              as city_name,
t1.customer_intent_city_seq                                               as city_seq,
t1.channel_id                                                             as channel_id,
t3.channel_name                                                           as channel_name,
t1.source                                                                 as source,
t1.source_tc                                                              as source_tc,
t1.from_source                                                            as from_source,

count(1)                                                                  as clue_num

from julive_dim.dim_clue_base_info t1 
left join julive_dim.dim_date t2 on regexp_replace(to_date(t1.create_time),"-","") = t2.skey 
left join julive_dim.dim_channel_info t3 on t1.channel_id = t3.channel_id 

where to_date(t1.create_time) >= '2017-01-01'

group by 
t2.date_str,
t2.date_str_zh,
t2.week_type,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.customer_intent_city_id,
t1.customer_intent_city_name,
t1.customer_intent_city_seq,
t1.channel_id,
t3.channel_name,
t1.source    ,
t1.source_tc ,
t1.from_source

),
tmp_distinct_city_channel_source_day as (

select 

t2.date_str                                                               as date_str,
t2.date_str_zh                                                            as date_str_zh,
t2.week_type                                                              as week_type,
t1.org_id                                                                 as org_id,    
t1.org_type                                                               as org_type,
t1.org_name                                                               as org_name,
t1.customer_intent_city_id                                                as city_id,
t1.customer_intent_city_name                                              as city_name,
t1.customer_intent_city_seq                                               as city_seq,
t1.channel_id                                                             as channel_id,
t3.channel_name                                                           as channel_name,
t1.source                                                                 as source,
t1.source_tc                                                              as source_tc,
t1.from_source                                                            as from_source,

sum(if(t1.is_distribute = 1,1,0))                                       as distribute_num

from julive_dim.dim_clue_base_info t1 
left join julive_dim.dim_date t2 on regexp_replace(to_date(t1.distribute_time),"-","") = t2.skey 
left join julive_dim.dim_channel_info t3 on t1.channel_id = t3.channel_id 

where to_date(t1.distribute_time) >= '2017-01-01'

group by 
t2.date_str,
t2.date_str_zh,
t2.week_type,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.customer_intent_city_id,
t1.customer_intent_city_name,
t1.customer_intent_city_seq,
t1.channel_id,
t3.channel_name,
t1.source    ,
t1.source_tc ,
t1.from_source

),
tmp_see_project_city_channel_source_day as (

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t2.week_type                                   as week_type,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.project_city_id                             as city_id,
t1.project_city_name                           as city_name,
t1.project_city_seq                            as city_seq,
t1.channel_id                                  as channel_id,
t3.channel_name                                as channel_name,
t1.source                                      as source,
t1.source_tc                                   as source_tc,
t1.from_source                                 as from_source,

sum(t1.see_num)                                as see_num,
sum(t1.see_project_num)                        as see_project_num

from julive_fact.fact_see_project_base_dtl t1 
left join julive_dim.dim_date t2 on regexp_replace(to_date(t1.plan_real_begin_time),"-","") = t2.skey 
left join julive_dim.dim_channel_info t3 on t1.channel_id = t3.channel_id 

group by 
t2.date_str,
t2.date_str_zh,
t2.week_type,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.project_city_id,
t1.project_city_name,
t1.project_city_seq,
t1.channel_id,
t3.channel_name,
t1.source    ,
t1.source_tc ,
t1.from_source


),
tmp_see_project_emp_city_channel_source_day as (

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t2.week_type                                   as week_type,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.emp_city_id                                 as emp_city_id,
t1.emp_city_name                               as emp_city_name,
t1.emp_city_seq                                as emp_city_seq,
t1.channel_id                                  as channel_id,
t3.channel_name                                as channel_name,
t1.source                                      as source,
t1.source_tc                                   as source_tc,
t1.from_source                                 as from_source,

sum(t1.see_num)                                as emp_city_see_num,
sum(t1.see_project_num)                        as emp_city_see_project_num

from julive_fact.fact_see_project_base_dtl t1 
left join julive_dim.dim_date t2 on regexp_replace(to_date(t1.plan_real_begin_time),"-","") = t2.skey 
left join julive_dim.dim_channel_info t3 on t1.channel_id = t3.channel_id 

group by 
t2.date_str,
t2.date_str_zh,
t2.week_type,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.emp_city_id,
t1.emp_city_name,
t1.emp_city_seq,
t1.channel_id,
t3.channel_name,
t1.source    ,
t1.source_tc ,
t1.from_source


),
tmp_subscribe_city_channel_source_day as (

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t2.week_type                                   as week_type,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.project_city_id                             as city_id,
t1.project_city_name                           as city_name,
t1.project_city_seq                            as city_seq,
t1.channel_id                                  as channel_id,
t3.channel_name                                as channel_name,
t1.source                                      as source,
t1.source_tc                                   as source_tc,
t1.from_source                                 as from_source,

sum(t1.subscribe_contains_cancel_ext_num)      as subscribe_contains_cancel_ext_num,
sum(t1.subscribe_contains_cancel_ext_amt)      as subscribe_contains_cancel_ext_amt,
sum(t1.subscribe_contains_cancel_ext_income)   as subscribe_contains_cancel_ext_income,
count(distinct t1.project_id)                  as subscribe_contains_cancel_ext_project_num,
sum(t1.subscribe_contains_ext_amt)             as subscribe_contains_ext_amt,
sum(t1.subscribe_contains_ext_income)          as subscribe_contains_ext_income,
sum(t1.subscribe_contains_ext_num)             as subscribe_contains_ext_num

from julive_fact.fact_subscribe_base_dtl t1 
left join julive_dim.dim_date t2 on regexp_replace(to_date(t1.subscribe_time),"-","") = t2.skey 
left join julive_dim.dim_channel_info t3 on t1.channel_id = t3.channel_id 

group by 
t2.date_str,
t2.date_str_zh,
t2.week_type,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.project_city_id,
t1.project_city_name,
t1.project_city_seq,
t1.channel_id,
t3.channel_name,
t1.source    ,
t1.source_tc ,
t1.from_source

),
tmp_subscribe_emp_city_channel_source_day as (

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t2.week_type                                   as week_type,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.emp_city_id                                 as emp_city_id,
t1.emp_city_name                               as emp_city_name,
t1.emp_city_seq                                as emp_city_seq,
t1.channel_id                                  as channel_id,
t3.channel_name                                as channel_name,
t1.source                                      as source,
t1.source_tc                                   as source_tc,
t1.from_source                                 as from_source,

sum(t1.subscribe_contains_cancel_ext_num)      as emp_city_subscribe_contains_cancel_ext_num,
sum(t1.subscribe_contains_cancel_ext_amt)      as emp_city_subscribe_contains_cancel_ext_amt,
sum(t1.subscribe_contains_cancel_ext_income)   as emp_city_subscribe_contains_cancel_ext_income,
count(distinct t1.project_id)                  as emp_city_subscribe_contains_cancel_ext_project_num,
sum(t1.subscribe_contains_ext_amt)             as emp_city_subscribe_contains_ext_amt,
sum(t1.subscribe_contains_ext_income)          as emp_city_subscribe_contains_ext_income,
sum(t1.subscribe_contains_ext_num)             as emp_city_subscribe_contains_ext_num

from julive_fact.fact_subscribe_base_dtl t1 
left join julive_dim.dim_date t2 on regexp_replace(to_date(t1.subscribe_time),"-","") = t2.skey 
left join julive_dim.dim_channel_info t3 on t1.channel_id = t3.channel_id 

group by 
t2.date_str,
t2.date_str_zh,
t2.week_type,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.emp_city_id,
t1.emp_city_name,
t1.emp_city_seq,
t1.channel_id,
t3.channel_name,
t1.source    ,
t1.source_tc ,
t1.from_source

),
tmp_back_subscribe_city_channel_source_day as (

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t2.week_type                                   as week_type,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.project_city_id                             as city_id,
t1.project_city_name                           as city_name,
t1.project_city_seq                            as city_seq,
t1.channel_id                                  as channel_id,
t3.channel_name                                as channel_name,
t1.source                                      as source,
t1.source_tc                                   as source_tc,
t1.from_source                                 as from_source,

sum(t1.subscribe_cancel_contains_ext_amt)      as subscribe_cancel_contains_ext_amt,
sum(t1.subscribe_cancel_contains_ext_num)      as subscribe_cancel_contains_ext_num,
sum(t1.subscribe_cancel_contains_ext_income)   as subscribe_cancel_contains_ext_income 

from julive_fact.fact_subscribe_base_dtl t1 
left join julive_dim.dim_date t2 on regexp_replace(to_date(t1.back_time),"-","") = t2.skey 
left join julive_dim.dim_channel_info t3 on t1.channel_id = t3.channel_id 

group by 
t2.date_str,
t2.date_str_zh,
t2.week_type,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.project_city_id,
t1.project_city_name,
t1.project_city_seq,
t1.channel_id,
t3.channel_name,
t1.source    ,
t1.source_tc ,
t1.from_source

),
tmp_back_subscribe_emp_city_channel_source_day as (

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t2.week_type                                   as week_type,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.emp_city_id                                 as emp_city_id,
t1.emp_city_name                               as emp_city_name,
t1.emp_city_seq                                as emp_city_seq,
t1.channel_id                                  as channel_id,
t3.channel_name                                as channel_name,
t1.source                                      as source,
t1.source_tc                                   as source_tc,
t1.from_source                                 as from_source,

sum(t1.subscribe_cancel_contains_ext_amt)      as emp_city_subscribe_cancel_contains_ext_amt,
sum(t1.subscribe_cancel_contains_ext_num)      as emp_city_subscribe_cancel_contains_ext_num,
sum(t1.subscribe_cancel_contains_ext_income)   as emp_city_subscribe_cancel_contains_ext_income 

from julive_fact.fact_subscribe_base_dtl t1 
left join julive_dim.dim_date t2 on regexp_replace(to_date(t1.back_time),"-","") = t2.skey 
left join julive_dim.dim_channel_info t3 on t1.channel_id = t3.channel_id 

group by 
t2.date_str,
t2.date_str_zh,
t2.week_type,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.emp_city_id,
t1.emp_city_name,
t1.emp_city_seq,
t1.channel_id,
t3.channel_name,
t1.source    ,
t1.source_tc ,
t1.from_source

),
tmp_sign_city_channel_source_day as (

select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t2.week_type                                   as week_type,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.project_city_id                             as city_id,
t1.project_city_name                           as city_name,
t1.project_city_seq                            as city_seq,
t1.channel_id                                  as channel_id,
t3.channel_name                                as channel_name,
t1.source                                      as source,
t1.source_tc                                   as source_tc,
t1.from_source                                 as from_source,

sum(t1.sign_contains_cancel_ext_num)           as sign_contains_cancel_ext_num,
sum(t1.sign_contains_cancel_ext_income)        as sign_contains_cancel_ext_income,
sum(t1.sign_contains_ext_income)               as sign_contains_ext_income,
sum(t1.sign_contains_cancel_ext_amt)           as sign_contains_cancel_ext_amt,
sum(t1.sign_contains_ext_num)                  as sign_contains_ext_num,
sum(t1.sign_cancel_contains_ext_num)           as sign_cancel_contains_ext_num

from julive_fact.fact_sign_base_dtl t1 
left join julive_dim.dim_date t2 on regexp_replace(to_date(t1.sign_time),"-","") = t2.skey 
left join julive_dim.dim_channel_info t3 on t1.channel_id = t3.channel_id 

where t1.audit_status != 2
group by 
t2.date_str,
t2.date_str_zh,
t2.week_type,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.project_city_id,
t1.project_city_name,
t1.project_city_seq,
t1.channel_id,
t3.channel_name,
t1.source    ,
t1.source_tc ,
t1.from_source

),
tmp_sign_emp_city_channel_source_day as (
select 

t2.date_str                                    as date_str,
t2.date_str_zh                                 as date_str_zh,
t2.week_type                                   as week_type,
t1.org_id                                      as org_id,    
t1.org_type                                    as org_type,
t1.org_name                                    as org_name,
t1.emp_city_id                                 as emp_city_id,
t1.emp_city_name                               as emp_city_name,
t1.emp_city_seq                                as emp_city_seq,
t1.channel_id                                  as channel_id,
t3.channel_name                                as channel_name,
t1.source                                      as source,
t1.source_tc                                   as source_tc,
t1.from_source                                 as from_source,

sum(t1.sign_contains_cancel_ext_num)           as emp_city_sign_contains_cancel_ext_num,
sum(t1.sign_contains_cancel_ext_income)        as emp_city_sign_contains_cancel_ext_income,
sum(t1.sign_contains_ext_income)               as emp_city_sign_contains_ext_income,
sum(t1.sign_contains_cancel_ext_amt)           as emp_city_sign_contains_cancel_ext_amt,
sum(t1.sign_contains_ext_num)                  as emp_city_sign_contains_ext_num,
sum(t1.sign_cancel_contains_ext_num)           as emp_city_sign_cancel_contains_ext_num

from julive_fact.fact_sign_base_dtl t1 
left join julive_dim.dim_date t2 on regexp_replace(to_date(t1.sign_time),"-","") = t2.skey 
left join julive_dim.dim_channel_info t3 on t1.channel_id = t3.channel_id 

where t1.audit_status != 2
group by 
t2.date_str,
t2.date_str_zh,
t2.week_type,
t1.org_id,  
t1.org_type,
t1.org_name,
t1.emp_city_id,
t1.emp_city_name,
t1.emp_city_seq,
t1.channel_id,
t3.channel_name,
t1.source    ,
t1.source_tc ,
t1.from_source

)  

-- 城市-渠道-天指标表 
insert overwrite table julive_fact.fact_city_channel_source_day_agg_base_indi

select 

coalesce(t1.date_str,t2.date_str,t3.date_str,t4.date_str,t6.date_str,t7.date_str,t10.date_str,t11.date_str,t12.date_str,t13.date_str)                         
        as date_str,
coalesce(t1.date_str_zh,t2.date_str_zh,t3.date_str_zh,t4.date_str_zh,t6.date_str_zh,t7.date_str_zh,t10.date_str_zh,t11.date_str_zh,t12.date_str_zh,t13.date_str_zh)       
        as date_str_zh,
coalesce(t1.week_type,t2.week_type,t3.week_type,t4.week_type,t6.week_type,t7.week_type,t10.week_type,t11.week_type,t12.week_type,t13.week_type)             
        as week_type,
coalesce(t1.org_id,t2.org_id,t3.org_id,t4.org_id,t6.org_id,t7.org_id,t10.org_id,t11.org_id,t12.org_id,t13.org_id)                         
        as org_id,    
coalesce(t1.org_type,t2.org_type,t3.org_type,t4.org_type,t6.org_type,t7.org_type,t10.org_type,t11.org_type,t12.org_type,t13.org_type)               
        as org_type,
coalesce(t1.org_name,t2.org_name,t3.org_name,t4.org_name,t6.org_name,t7.org_name,t10.org_name,t11.org_name,t12.org_name,t13.org_name)                  
        as org_name,
t8.region                                                                                                 as region,
t8.bd_region                                                                                              as bd_region, 
t8.bd_warregion                                                                                           as bd_warregion,       
t8.mgr_city_seq                                                                                           as mgr_city_seq,  
t8.mgr_city                                                                                               as mgr_city,      
coalesce(t1.city_id,t2.city_id,t3.city_id,t4.city_id,t6.city_id,t7.city_id)                               as city_id,
coalesce(t1.city_name,t2.city_name,t3.city_name,t4.city_name,t6.city_name,t7.city_name)                   as city_name,
coalesce(t1.city_seq,t2.city_seq,t3.city_seq,t4.city_seq,t6.city_seq,t7.city_seq)                         as city_seq,
coalesce(t10.emp_city_id,t11.emp_city_id,t12.emp_city_id,t13.emp_city_id)
        as emp_city_id,
coalesce(t10.emp_city_name,t11.emp_city_name,t12.emp_city_name,t13.emp_city_name)
        as emp_city_name,
coalesce(t10.emp_city_seq,t11.emp_city_seq,t12.emp_city_seq,t13.emp_city_seq)
        as emp_city_seq,
coalesce(t1.channel_id,t2.channel_id,t3.channel_id,t4.channel_id,t6.channel_id,t7.channel_id,t10.channel_id,t11.channel_id,t12.channel_id,t13.channel_id)     
        as channel_id,
coalesce(t1.channel_name,t2.channel_name,t3.channel_name,t4.channel_name,t6.channel_name,t7.channel_name,t10.channel_name,t11.channel_name,t12.channel_name,t13.channel_name) 
        as channel_name,
coalesce(t1.source,t2.source,t3.source,t4.source,t6.source,t7.source,t10.source,t11.source,t12.source,t13.source)                             
        as source,
coalesce(t1.source_tc,t2.source_tc,t3.source_tc,t4.source_tc,t6.source_tc,t7.source_tc,t10.source_tc,t11.source_tc,t12.source_tc,t13.source_tc)             
        as source_tc,
t5.media_id,
t5.media_name,
t5.module_id,
t5.module_name,
t5.device_id,
t5.device_name,

t1.clue_num,
t7.distribute_num,

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

t4.sign_contains_cancel_ext_num,
t4.sign_contains_cancel_ext_income,
t4.sign_contains_ext_income,
t4.sign_contains_cancel_ext_amt,
t4.sign_contains_ext_num,
t4.sign_cancel_contains_ext_num,
t12.emp_city_sign_contains_cancel_ext_num,
t12.emp_city_sign_contains_cancel_ext_income,
t12.emp_city_sign_contains_ext_income,
t12.emp_city_sign_contains_cancel_ext_amt,
t12.emp_city_sign_contains_ext_num,
t12.emp_city_sign_cancel_contains_ext_num,
coalesce(t1.from_source,t2.from_source,t3.from_source,t4.from_source,t6.from_source,t7.from_source,t10.from_source,t11.from_source,t12.from_source,t13.from_source)
        as from_source,

current_timestamp() as etl_time 

from tmp_clue_city_channel_source_day t1 -- 线索/上户 
full join tmp_see_project_city_channel_source_day t2 on t1.date_str = t2.date_str and t1.city_id = t2.city_id and t1.channel_id = t2.channel_id and t1.source = t2.source and t1.from_source = t2.from_source and t1.org_id = t2.org_id 
full join tmp_subscribe_city_channel_source_day t3 on t1.date_str = t3.date_str and t1.city_id = t3.city_id and t1.channel_id = t3.channel_id and t1.source = t3.source and t1.from_source = t3.from_source and t1.org_id = t3.org_id 
full join tmp_sign_city_channel_source_day t4 on t1.date_str = t4.date_str and t1.city_id = t4.city_id and t1.channel_id = t4.channel_id and t1.source = t4.source and t1.from_source = t4.from_source and t1.org_id = t4.org_id 
full join tmp_back_subscribe_city_channel_source_day t6 on t1.date_str = t6.date_str and t1.city_id = t6.city_id and t1.channel_id = t6.channel_id and t1.source = t6.source and t1.from_source = t6.from_source and t1.org_id = t6.org_id 
full join tmp_distinct_city_channel_source_day t7 on t1.date_str = t7.date_str and t1.city_id = t7.city_id and t1.channel_id = t7.channel_id and t1.source = t7.source and t1.from_source = t7.from_source and t1.org_id = t7.org_id 
full join tmp_see_project_emp_city_channel_source_day t10 on t1.date_str = t10.date_str and t1.city_id = t10.emp_city_id and t1.channel_id = t10.channel_id and t1.source = t10.source and t1.from_source = t10.from_source and t1.org_id = t10.org_id 
full join tmp_subscribe_emp_city_channel_source_day t11 on t1.date_str = t11.date_str and t1.city_id = t11.emp_city_id and t1.channel_id = t11.channel_id and t1.source = t11.source and t1.from_source = t11.from_source and t1.org_id = t11.org_id 
full join tmp_back_subscribe_emp_city_channel_source_day t13 on t1.date_str = t13.date_str and t1.city_id = t13.emp_city_id and t1.channel_id = t13.channel_id and t1.source = t13.source and t1.from_source = t13.from_source and t1.org_id = t13.org_id 
full join tmp_sign_emp_city_channel_source_day t12 on t1.date_str = t12.date_str and t1.city_id = t12.emp_city_id and t1.channel_id = t12.channel_id and t1.source = t12.source and t1.from_source = t12.from_source and t1.org_id = t12.org_id 


left join julive_dim.dim_city t8 on coalesce(t1.city_id,t2.city_id,t3.city_id,t4.city_id,t6.city_id,t7.city_id,t10.emp_city_id,t11.emp_city_id,t12.emp_city_id,t13.emp_city_id) = t8.city_id 

left join julive_dim.dim_channel_info t5 on coalesce(t1.channel_id,t2.channel_id,t3.channel_id,t4.channel_id,t6.channel_id,t7.channel_id,t10.channel_id,t11.channel_id,t12.channel_id,t13.channel_id) = t5.channel_id 
;

insert overwrite table julive_fact.fact_city_channel_source_day_agg_indi 
select * from julive_fact.fact_city_channel_source_day_agg_base_indi
where from_source =1;

insert overwrite table julive_fact.fact_wlmq_city_channel_source_day_agg_indi 
select * from julive_fact.fact_city_channel_source_day_agg_base_indi
where from_source =2;

insert overwrite table julive_fact.fact_esf_city_channel_source_day_agg_indi 
select * from julive_fact.fact_city_channel_source_day_agg_base_indi
where from_source =3;

insert overwrite table julive_fact.fact_jms_city_channel_source_day_agg_indi 
select * from julive_fact.fact_city_channel_source_day_agg_base_indi
where from_source =4;

insert into table julive_fact.fact_jms_city_channel_source_day_agg_indi 
select * from julive_fact.fact_city_channel_source_day_agg_base_indi
where from_source =1 and org_id !=48;
