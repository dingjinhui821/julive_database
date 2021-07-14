
-- 衍生指标命名规则：
-- sign_status                       int            COMMENT '签约状态:1已网签 2退网签', 
-- sign_type                         int            COMMENT '成交类型:1合作 4外联', 
-- 
-- sign_status in (1,2) and sign_type in (1,4) -- sign_contains_cancel_ext
-- sign_status in (1,2) and sign_type in (1)   -- sign_contains_cancel
-- 
-- sign_status = 1 and sign_type in (1,4)      -- sign_contains_ext
-- sign_status = 1 and sign_type in (1)        -- sign_coop
-- 
-- sign_status = 2 and sign_type in (1,4)      -- sign_cancel_contains_ext
-- sign_status = 2 and sign_type in (1)        -- sign_cancel_coop

-- num : 量 
-- amt ：金额 
-- income ：收入 



-- ---------------------------------------------------------------
-- 签约事实表ETL 

set hive.execution.engine=spark;
set spark.app.name=fact_sign_dtl; 
set mapred.job.name=fact_sign_dtl;
set spark.yarn.queue=etl;

insert overwrite table julive_fact.fact_sign_base_dtl

select 

tmp.id                                                                          as sign_id,
tmp.org_id                                                                      as org_id,
d6.org_type                                                                     as org_type,
d7.team_name                                                                    as org_name,
tmp.deal_id                                                                     as deal_id,
tmp.order_id                                                                    as clue_id,  
clue.channel_id                                                                 as channel_id,-- 20191015 
tmp.subscribe_id                                                                as subscribe_id,
tmp.emp_id                                                                      as emp_id,
d8.emp_name                                                                     as emp_name,
tmp.emp_mgr_id                                                                  as emp_mgr_id,
tmp.emp_mgr_name                                                                as emp_mgr_name,

d2.region                                                                       as region,
d2.mgr_city_seq                                                                 as mgr_city_seq,
d2.mgr_city                                                                     as mgr_city,

tmp.city_id                                                                     as city_id,
if(d1.city_name is not null,d1.city_name,d4.city_name)                          as city_name,
if(d1.city_seq is not null,d1.city_seq,d4.city_seq)                             as city_seq,

clue.customer_intent_city_id                                                    as customer_intent_city_id,-- 20191015 
clue.customer_intent_city_name                                                  as customer_intent_city_name,-- 20191015 
clue.customer_intent_city_seq                                                   as customer_intent_city_seq,-- 20191015 
clue.source                                                                     as source,
clue.source_tc                                                                  as source_tc,

project.city_id                                                                 as project_city_id,
project.city_name                                                               as project_city_name,
d2.city_seq                                                                     as project_city_seq,

                      
if(from_unixtime(tmp.sign_date,'yyyyMMdd')>='20180101' and d8.adjust_city_id is not null,d9.region,d2.region)               as emp_region,--20210316
if(from_unixtime(tmp.sign_date,'yyyyMMdd')>='20180101' and d8.adjust_city_id is not null,d9.mgr_city_seq,d2.mgr_city_seq)   as emp_mgr_city_seq, --20210316
if(from_unixtime(tmp.sign_date,'yyyyMMdd')>='20180101' and d8.adjust_city_id is not null,d9.mgr_city,d2.mgr_city)           as emp_mgr_city,--20210316
if(from_unixtime(tmp.sign_date,'yyyyMMdd')>='20180101' and d8.adjust_city_id is not null,d8.adjust_city_id,project.city_id) as emp_city_id,--20210316
if(from_unixtime(tmp.sign_date,'yyyyMMdd')>='20180101' and d8.adjust_city_id is not null,d9.city_name,project.city_name)    as emp_city_name,--20210316
if(from_unixtime(tmp.sign_date,'yyyyMMdd')>='20180101' and d8.adjust_city_id is not null,d9.city_seq,d2.city_seq)           as emp_city_seq,--20210316
 
tmp.user_id                                                                     as user_id,
clue.user_name                                                                  as user_name,

tmp.project_id                                                                  as project_id,
tmp.project_name                                                                as project_name,
tmp.house_type                                                                  as house_type,
tmp.acreage                                                                     as acreage,
tmp.house_number                                                                as house_number,

tmp.status                                                                      as sign_status,
tmp.sign_type                                                                   as sign_type,
tmp.audit_status                                                                as audit_status,
tmp.subscribe_status                                                            as subscribe_status, -- 20191016添加 
tmp.subscribe_date                                                              as subscribe_date, -- 20191016添加 

-- 原始指标 
tmp.deal_money                                                                  as orig_deal_amt, -- 成交金额 
tmp.forecast_payment_total                                                      as orig_sign_income, -- 每单签约收入金额 
if(tmp.status = 1,1,0)                                                          as orig_sign_num, --签约指标
if(tmp.status in (1,2),1,0)                                                     as orig_sign_contains_cancel_num, --签约含退指标

-- 金额 
if(tmp.status in (1,2) and tmp.sign_type in (1,4),tmp.deal_money,0)             as sign_contains_cancel_ext_amt,
if(tmp.status in (1,2) and tmp.sign_type in (1,4),tmp.forecast_payment_total,0) as sign_contains_cancel_ext_income,
if(tmp.status in (1,2) and tmp.sign_type in (1),tmp.deal_money,0)               as sign_contains_cancel_amt, -- 签约-含退、不含外联GMV 
if(tmp.status in (1,2) and tmp.sign_type in (1),tmp.forecast_payment_total,0)   as sign_contains_cancel_income, -- 签约-含退、不含外联收入(佣金)
if(tmp.status = 1 and tmp.sign_type in (1,4),tmp.deal_money,0)                  as sign_contains_ext_amt,
if(tmp.status = 1 and tmp.sign_type in (1,4),tmp.forecast_payment_total,0)      as sign_contains_ext_income,
if(tmp.status = 1 and tmp.sign_type in (1),tmp.deal_money,0)                    as sign_coop_amt,
if(tmp.status = 1 and tmp.sign_type in (1),tmp.forecast_payment_total,0)        as sign_coop_income,
if(tmp.status = 2 and tmp.sign_type in (1,4),tmp.deal_money,0)                  as sign_cancel_contains_ext_amt,
if(tmp.status = 2 and tmp.sign_type in (1,4),tmp.forecast_payment_total,0)      as sign_cancel_contains_ext_income, -- 退签约佣金-含外联(佣金)
if(tmp.status = 2 and tmp.sign_type in (1),tmp.deal_money,0)                    as sign_cancel_coop_amt,

-- 量 
if(tmp.status in (1,2) and tmp.sign_type in (1,4),1,0)                          as sign_contains_cancel_ext_num,
if(tmp.status in (1,2) and tmp.sign_type in (1),1,0)                            as sign_contains_cancel_num,
if(tmp.status = 1 and tmp.sign_type in (1),1,0)                                 as sign_coop_num,
if(tmp.status = 1 and tmp.sign_type in (1,4),1,0)                               as sign_contains_ext_num,
if(tmp.status = 2 and tmp.sign_type in (1),1,0)                                 as sign_cancel_coop_num,
if(tmp.status = 2 and tmp.sign_type in (1,4),1,0)                               as sign_cancel_contains_ext_num,

if(tmp.audit_status = 1,1,0)                                                    as audit_num,

from_unixtime(tmp.sign_date)                                                    as sign_time,
from_unixtime(tmp.submit_review_datetime)                                       as submit_review_time,

-- if(d3.id is null and d5.id is null,1,
--    if(d3.id is not null,2,
--       if(d5.id is not null,3,999)))                                             as from_source,
case 
when d3.id is not null then 2 -- 乌鲁木齐数据 
when d5.id is not null then 3 -- 二手房中介数据 
when (d6.org_type != 1 and d6.join_type not in (0,1,2) and d6.id is not null) then 4 -- 加盟商数据 
else 1 -- 居理数据 
end                                                                             as from_source, -- 数据来源
from_unixtime(tmp.create_date)                                                  as create_time,
current_timestamp()                                                             as etl_time

from (

select 

temp.deal_id,
temp.order_id,
temp.subscribe_id,
temp.project_id,
temp.deal_money,
temp.project_name,
temp.user_id,
temp.emp_id,
temp.emp_mgr_id,
temp.emp_mgr_name,
temp.sign_name,
temp.id,
temp.sign_type,
temp.house_type,
temp.acreage,
temp.house_number,
temp.status,
temp.payment,
temp.review,
temp.city_id,
temp.sign_date,
temp.submit_review_datetime,
temp.create_date,
temp.audit_status,
temp.auditor,
temp.subscribe_status,
temp.subscribe_date,
temp.forecast_payment_total,
temp.org_id

from (

select 
 
tmp1.deal_id,
tmp1.order_id,
tmp1.subscribe_id,
tmp1.project_id,
tmp1.deal_money,
tmp1.project_name,
tmp1.user_id,
tmp1.emp_id,
tmp1.emp_mgr_id,
tmp1.emp_mgr_name,
tmp1.sign_name,
tmp1.id,
tmp1.sign_type,
tmp1.house_type,
coalesce(tmp2.acreage,tmp2.ac_acreage) as acreage,
tmp1.house_number,
tmp1.status,
tmp1.payment,
tmp1.review,
tmp1.city_id,
tmp1.sign_date,
tmp1.submit_review_datetime,
tmp1.create_date,
tmp1.audit_status,
tmp1.auditor,
tmp3.status as subscribe_status,
from_unixtime(tmp3.subscribe_datetime,'yyyy-MM-dd') as subscribe_date,
tmp2.forecast_payment_total,
tmp1.org_id,
row_number()over(partition by tmp1.deal_id order by 1) as rn 

from (

select 

t1.deal_id,
t1.order_id,
t1.subscribe_id,
t1.project_id,
t1.deal_money,
t1.project_name,
t1.user_id,
t1.employee_id as emp_id,
t1.employee_manager_id as emp_mgr_id,
t1.employee_manager_name as emp_mgr_name,
t1.sign_name,
t1.id,
t1.sign_type,
t1.city_id,
t1.house_type,
t1.acreage,
t1.house_number,
t1.status,
t1.payment,
t1.review,
t1.submit_review_datetime,
t1.sign_datetime as sign_date,
t1.create_datetime as create_date,

t2.audit_status,
t2.auditor,
t1.org_id

from  
ods.yw_sign  t1 
join ( -- 取签约审核状态 

select  

t.order_id,
t.auditor,
t.audit_status,
t.sign_id 

from (

select

sign_id,
order_id,
audit_status,
audit_role,
auditor,
row_number() over(partition by sign_id order by submit_audit_datetime desc) as rn 

from ods.yw_sign_audit_record 
where audit_role = 'bd_manager'

) t 
where t.rn = 1 

) t2 on t1.id = t2.sign_id 
where t1.status != -1

) tmp1 
left join ods.yw_deal tmp2 on tmp1.deal_id = tmp2.id
left join ods.yw_subscribe tmp3 on tmp1.deal_id = tmp3.deal_id 
where tmp3.status = 1 -- bdp逻辑取已认购

) temp 
where temp.rn = 1 

) tmp 
left join julive_dim.dim_city d1 on d1.city_id = tmp.city_id 
left join julive_dim.dim_clue_base_info clue on tmp.order_id = clue.clue_id 
left join julive_dim.dim_project_info project on tmp.project_id = project.project_id and project.end_date = '9999-12-31'
left join julive_dim.dim_city d2 on d2.city_id = project.city_id 
left join ods.yw_developer_city_config d3 on project.city_id = d3.city_id
left join julive_dim.dim_wlmq_city d4 on d4.city_id = project.city_id
left join ods.yw_esf_virtual_config d5 on d5.virtual_city = project.city_id
left join ods.yw_org_info d6 on tmp.org_id = d6.org_id -- 加盟商 ：20201015集成加盟商数据 
left join (
     select 
     department_id,
      team_name
  from (select
      department_id,
      team_name,
      row_number() over (partition by department_id order by effective_date desc) as rn
      from ods.yw_department_architecture_history
      where pid =0 
  ) a 
  where rn = 1
) d7 on tmp.org_id = d7.department_id
left join julive_dim.dim_employee_base_info d8 on tmp.emp_id =d8.emp_id and regexp_replace(to_date(from_unixtime(tmp.sign_date)),'-','') = d8.pdate
left join julive_dim.dim_city d9 on d8.adjust_city_id =d9.city_id

;



insert overwrite table julive_fact.fact_sign_dtl_base
select * from julive_fact.fact_sign_base_dtl
where from_source =1 ;

insert overwrite table julive_fact.fact_sign_dtl
select * from julive_fact.fact_sign_base_dtl
where from_source=1 and audit_status != 2 ;

insert overwrite table julive_fact.fact_wlmq_sign_dtl
select * from julive_fact.fact_sign_base_dtl
where from_source=2 and audit_status != 2 ;

insert overwrite table julive_fact.fact_esf_sign_dtl
select * from julive_fact.fact_sign_base_dtl
where from_source=3 and audit_status != 2 ;

insert overwrite table julive_fact.fact_jms_sign_dtl
select * from julive_fact.fact_sign_base_dtl
where from_source=4 and audit_status != 2 ;

insert into table julive_fact.fact_jms_sign_dtl
select 
* 
from julive_fact.fact_sign_base_dtl
where from_source=1 and audit_status != 2 and org_id != 48 ;


