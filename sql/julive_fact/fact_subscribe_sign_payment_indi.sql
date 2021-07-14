------------------------------------------------------------------------------------
-- ETL -----------------------------------------------------------------------------
------------------------------------------------------------------------------------
set etl_date = concat_ws('-',substr(${hiveconf:etlDate},1,4),substr(${hiveconf:etlDate},5,2),substr(${hiveconf:etlDate},7,2));
---set etl_date = date_add(current_date(),-1); -- 測試用 


set spark.app.name=fact_subscribe_sign_payment_indi; 
set hive.execution.engine=spark;
set spark.yarn.queue=etl;
set spark.default.parallelism=1400;
set spark.executor.cores=2;
set spark.executor.memory=4g;
set spark.executor.instances=14;
set spark.yarn.executor.memoryOverhead=4096;
set hive.prewarm.enabled=true;
set hive.prewarm.numcontainers=14;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000; 

with tmp_subscribe_order as (--已认购订单

select 
subscribe_id,
clue_id,
deal_id,
project_id,
project_name,
emp_id,      
emp_name,
project_city_id,  
project_city_name,
project_city_seq,
subscribe_status, 
subscribe_type,
orig_subsctibe_income,
orig_deal_amt,
to_date(subscribe_time) as subscribe_date,
to_date(back_time) as back_date
from julive_fact.fact_subscribe_dtl
where to_date(subscribe_time)<=${hiveconf:etl_date}
),
tmp_grass_signed_order as (---已草签订单 ok

select 
sign_id as grass_sign_id,
deal_id,
sign_status as grass_sign_status,
to_date(grass_sign_time) as grass_sign_date
from julive_fact.fact_grass_sign_dtl
where to_date(grass_sign_time)<=${hiveconf:etl_date}
),
tmp_signed_order as ( --已签约订单 ok
select 
temp.deal_id,
temp.sign_id,
temp.sign_type,
temp.sign_status,
temp.sign_date,
temp.submit_audit_date,
temp.audit_status,
temp.auditor


from (

select 

t1.id as sign_id,
t1.deal_id,
t1.sign_type,
t1.status as sign_status,
from_unixtime(t1.sign_datetime,"yyyy-MM-dd") as sign_date,
t2.audit_status,
t2.auditor,
t2.submit_audit_date,
row_number()over(partition by t1.deal_id order by 1) as rn 


from ods.yw_sign t1 
left join ( -- 取签约审核状态 

select  

t.order_id,
t.auditor,
t.audit_status,
t.sign_id,
t.submit_audit_date

from (

select

sign_id,
order_id,
audit_status,
audit_role,
auditor,
from_unixtime(submit_audit_datetime,"yyyy-MM-dd") as submit_audit_date,
row_number() over(partition by sign_id order by submit_audit_datetime desc) as rn 

from ods.yw_sign_audit_record 
where audit_role = 'bd_manager' 
   and from_unixtime(submit_audit_datetime,"yyyy-MM-dd")<=${hiveconf:etl_date}

) t 
where t.rn = 1 

) t2 on t1.id = t2.sign_id 
where t1.status != -1

) temp 
where temp.rn = 1 
  and to_date(temp.sign_date)<=${hiveconf:etl_date}
),
tmp_plan_sign_order as (---预计签约时间

select 
deal_id,
plan_sign_date,
is_have_risk
from 

(select  
id,
deal_id,
is_have_risk,
from_unixtime(plan_sign_datetime,"yyyy-MM-dd") as plan_sign_date,
row_number()over(partition by deal_id order by create_datetime desc) as rn
from ods.yw_sign_risk
where from_unixtime(create_datetime,"yyyy-MM-dd")<=${hiveconf:etl_date}
) a1
where rn=1

),
tmp_payment_order as(---已回款订单
select 
t1.deal_id,
t1.actual_time,
t2.actual_amt

from 

(select 
payment_id,
deal_id,
actual_time,
row_number()over(partition by deal_id order by actual_time desc ) as rn
from julive_fact.fact_payment_dtl a1
where to_date(actual_time)<=${hiveconf:etl_date}
) t1

left join
(select
deal_id,
sum(actual_amt) as actual_amt
from julive_fact.fact_payment_dtl a2
where to_date(actual_time)<=${hiveconf:etl_date}
group by deal_id
) t2

on t1.deal_id=t2.deal_id
where t1.rn=1
)


insert overwrite table julive_fact.fact_subscribe_sign_payment_indi partition(pdate)

select  
t1.deal_id,                
t1.subscribe_id,           
t2.sign_id,                 
t3.grass_sign_id,           
t1.clue_id,                
                        
t1.project_city_id,        
t1.project_city_name,       
t1.project_city_seq,  
 
t10.mgr_city as mgr_city_name,
t10.mgr_city_seq, 
t10.region,  

t1.project_id,              
t1.project_name,            
t5.project_type,            
                        
t1.emp_id,                  
t1.emp_name,                
t7.emp_id as clue_emp_id,             
t7.emp_name as clue_emp_name,

t8.direct_leader_id,        
t8.direct_leader_name,      
t8.indirect_leader_id,      
t8.indirect_leader_name,    
t9.direct_leader_id  as now_direct_leader_id,    
t9.direct_leader_name as now_direct_leader_name,
t9.indirect_leader_id as now_indirect_leader_id,  
t9.indirect_leader_name as now_indirect_leader_name,

t1.subscribe_status,        
t1.subscribe_type,          
t2.sign_status,             
t2.sign_type,               
t3.grass_sign_status, 
t2.audit_status as sign_audit_status,

t6.is_have_risk,
 
t1.orig_subsctibe_income as receive_amt, 
t1.orig_deal_amt         as orig_deal_amt,        
t4.actual_amt,              
t7.total_price_max,
t7.qualifications,           

t1.subscribe_date,          
t2.sign_date,               
t4.actual_time as actual_date,             
t3.grass_sign_date,         
t1.back_date,               
t6.plan_sign_date,

current_timestamp()                                as etl_time,
regexp_replace(${hiveconf:etl_date},'-','')                as pdate



from tmp_subscribe_order t1
left join  tmp_signed_order t2                on  t1.deal_id=t2.deal_id
left join  tmp_grass_signed_order t3          on t1.deal_id=t3.deal_id
left join  tmp_payment_order t4               on t1.deal_id=t4.deal_id
left join  ods.cj_project t5                  on t1.project_id=t5.project_id
left join  tmp_plan_sign_order t6             on t1.deal_id=t6.deal_id
left join julive_dim.dim_clue_info t7         on t1.clue_id=t7.clue_id
left join julive_dim.dim_ps_employee_info t8  on t7.emp_id=t8.emp_id and t8.pdate=regexp_replace(t1.subscribe_date,'-','')
left join  
(
select t.*,
row_number()over(partition by t.emp_id order by t.pdate desc) as rn
from julive_dim.dim_ps_employee_info t)  t9   on t7.emp_id=t9.emp_id  and t9.rn=1
left join julive_dim.dim_city  t10     on    t1.project_city_id=t10.city_id
;

