
set hive.execution.engine=spark;
set spark.app.name=fact_queue_dtl;
set mapred.job.name=fact_queue_dtl;
set spark.yarn.queue=etl;
insert overwrite table julive_fact.fact_queue_dtl 

select 

t1.id                           as queue_id,
t1.order_id                     as clue_id,
t3.channel_id                   as channel_id, -- 20191015 
t1.deal_id                      as deal_id,
t1.employee_id                  as emp_id,
''                              as emp_name,
t1.employee_manager_id          as emp_mgr_id,
t1.employee_manager_name        as emp_mgr_name,
t1.user_id                      as user_id,
t1.user_name                    as user_name,
t1.project_id                   as project_id,
t1.project_name                 as project_name,

t1.city_id                      as city_id,
t2.city_name                    as city_name,
t2.city_seq                     as city_seq,

t3.customer_intent_city_id      as customer_intent_city_id,-- 20191015 
t3.customer_intent_city_name    as customer_intent_city_name,-- 20191015 
t3.customer_intent_city_seq     as customer_intent_city_seq,-- 20191015 

t4.city_id                      as project_city_id,
t4.city_name                    as project_city_name,
t5.city_seq                     as city_seq,

t1.house_type                   as house_type,
t1.acreage                      as acreage,
t1.house_number                 as house_number,

t1.status                       as queue_status,
t1.deal_type                    as queue_type,

t1.deal_money                   as orig_deal_amt,
t1.discount                     as discount,
t1.cost                         as cost,
if(t1.status = 3,1,0)           as orig_queue_num,
if(t1.status in (3,4),1,0)      as orig_queue_contains_cancel_num,

-- 量
if(t1.status = 3 and t1.deal_type in (1),1,0)      as queue_coop_num,
if(t1.status = 3 and t1.deal_type in (1,4),1,0)    as queue_contains_ext_num,
if(t1.status = 4 and t1.deal_type in (1),1,0)      as queue_coop_cancel_num,
if(t1.status = 4 and t1.deal_type in (1,4),1,0)    as queue_contains_ext_cancel_num,

from_unixtime(t1.paihao_datetime)                  as queue_time,
from_unixtime(t1.create_datetime)                  as create_time,
current_timestamp()                                as etl_time

from ods.yw_paihao t1
-- left join ods.cj_district t2 on t1.city_id = t2.id 
left join julive_dim.dim_city t2 on t1.city_id = t2.city_id 
left join julive_dim.dim_clue_info t3 on t1.order_id = t3.clue_id 
left join julive_dim.dim_project_info t4 on t1.project_id = t4.project_id and t4.end_date = '9999-12-31' 
left join julive_dim.dim_city t5 on t4.city_id = t5.city_id 

where t1.status != -1 
;

