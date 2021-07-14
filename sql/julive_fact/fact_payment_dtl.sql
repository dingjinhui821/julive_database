set hive.execution.engine=spark;
 set spark.app.name=fact_payment_dtl;  
 set spark.yarn.queue=etl;
 set spark.executor.cores=2;
 set spark.executor.memory=2g;
 set spark.executor.instances=12;
 set spark.yarn.executor.memoryOverhead=1024; 
 set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table julive_fact.fact_payment_base_dtl 

select 

tmp1.id                                                       as payment_id,
tmp1.forecast_id                                              as forecast_id,
tmp1.contract_id                                              as contract_id,
tmp1.category_id                                              as category_id,
tmp1.deal_id                                                  as deal_id,
tmp1.clue_id                                                  as clue_id,
tmp1.channel_id                                               as channel_id,
tmp1.project_id                                               as project_id,
tmp1.project_name                                             as project_name,
tmp2.city_id                                                  as city_id,
if(tmp3.city_name is not null,tmp3.city_name,tmp5.city_name) as city_name,
if(tmp3.city_seq is not null,tmp3.city_seq,tmp5.city_seq)     as city_seq,
from_unixtime(tmp1.receive_datetime)                          as receive_time,
tmp1.receive_money                                            as receive_amt,
from_unixtime(tmp1.actual_datetime)                           as actual_time,
tmp1.actual_money                                             as actual_amt,
tmp1.commission_type                                          as commission_type,
tmp1.step                                                     as step,
tmp1.audit_date                                               as audit_date,
tmp1.payback_emp_id                                           as payback_emp_id,
tmp1.auditor_id                                               as auditor_id,
if(tmp4.id is null and tmp6.id is null,1,
   if(tmp4.id is not null,2,
      if(tmp6.id is not null,3,999)))                         as from_source, 
from_unixtime(tmp1.create_datetime)                           as create_time,
from_unixtime(tmp1.update_datetime)                           as update_time,
current_timestamp()                                           as etl_time 

from (

select 

t1.id,
t1.forecast_id,
t1.contract_id,
t1.category_id,
t1.deal_id,

t2.order_id as clue_id, -- 20191015 
t3.channel_id, -- 20191015 
t2.project_id, -- 20191015 
t2.project_name, -- 20191015 

t1.receive_datetime,
t1.receive_money,
t1.actual_datetime,
t1.actual_money,
t1.commission_type,
t1.step,
from_unixtime(t1.audit_datetime,'yyyy-MM-dd') as audit_date, -- 20191015 
t1.payback_employee_id as payback_emp_id, -- 20191015 
t1.auditor as auditor_id, -- 20191015 
t1.create_datetime,
t1.update_datetime

from ods.ex_payment_detail t1 
left join ods.yw_deal t2 on t1.deal_id = t2.id 
left join julive_dim.dim_clue_base_info t3 on t2.order_id = t3.clue_id 
where t1.status = 1 

) tmp1 join (

select 

t1.deal_id,
t3.city_id 

from 
ods.yw_sign  t1 
join ( -- 签约已审核 

select 

t.sign_id      as sign_id, -- pk 
t.order_id     as order_id, 
t.auditor      as auditor,
t.audit_status as audit_status


from (

select

order_id,
audit_status,
sign_id as sign_id,
audit_role,
auditor,
row_number() over(partition by sign_id order by submit_audit_datetime desc) as rn  

from ods.yw_sign_audit_record
where audit_role ="bd_manager"
and audit_status != 2 

) t 
where t.rn = 1 

) t2 on t1.id = t2.sign_id
join ( -- 已认购 

select t1.deal_id,t1.city_id
from 
 ods.yw_subscribe  t1 
where t1.status = 1 
group by t1.deal_id,t1.city_id

) t3 on t1.deal_id = t3.deal_id
where t1.status != -1
group by t1.deal_id,t3.city_id  

) tmp2 on tmp1.deal_id = tmp2.deal_id 
-- left join ods.cj_district tmp3 on tmp3.id = tmp2.city_id
left join julive_dim.dim_city tmp3 on tmp3.city_id = tmp2.city_id 
left join ods.yw_developer_city_config tmp4 on tmp4.city_id = tmp2.city_id
left join julive_dim.dim_wlmq_city tmp5 on tmp2.city_id = tmp5.city_id
left join ods.yw_esf_virtual_config tmp6 on tmp2.city_id = tmp6.virtual_city
;

insert overwrite table julive_fact.fact_payment_dtl 
select * from julive_fact.fact_payment_base_dtl
where from_source =1;

insert overwrite table julive_fact.fact_wlmq_payment_dtl 
select * from julive_fact.fact_payment_base_dtl
where from_source =2;

insert overwrite table julive_fact.fact_esf_payment_dtl 
select * from julive_fact.fact_payment_base_dtl
where from_source =3;
