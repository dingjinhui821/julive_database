
set hive.execution.engine=spark;
set spark.app.name=fact_see_project_base_dtl;
set mapred.job.name=fact_see_project_base_dtl;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;
set hive.exec.dynamic.partition.mode=nonstrict;


-- ------------------------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------------------------
-- 基表加工逻辑 
insert overwrite table julive_fact.fact_see_project_base_dtl 

select 

coalesce(t2.id,max(t2.id)over() + t1.id)                 as see_project_id, -- 主键 
t1.id                                                    as see_id, 
from_unixtime(t1.create_datetime,'yyyy-MM-dd')           as see_create_date, -- 20191015 
t1.order_id                                              as clue_id,
if(t1.org_id is null,48,t1.org_id)                       as org_id,  --20201224添加
if(t12.org_type is null,1,t12.org_type)                  as org_type,--20201224添加
if(t13.team_name is null,'北京居理科技有限公司',t13.team_name)   as org_name,--20201224添加
t4.channel_id                                            as channel_id, -- 20191015 

t1.city_id                                               as city_id,
if(t3.city_name is not null,t3.city_name,t9.city_name)   as city_name,
if(t3.city_seq is not null,t3.city_seq,t9.city_seq)      as city_seq,

t4.customer_intent_city_id                               as customer_intent_city_id,-- 20191015 
t4.customer_intent_city_name                             as customer_intent_city_name,-- 20191015 
t4.customer_intent_city_seq                              as customer_intent_city_seq,-- 20191015 
t4.source                                                as source,
t4.source_tc                                             as source_tc,
t8.region                                                as region,
t8.mgr_city_id                                           as mgr_city_id,--20201030
t8.mgr_city                                              as mgr_city,--20201030
t7.city_id                                               as project_city_id,-- 20191015 
t7.city_name                                             as project_city_name,-- 20191015 
t8.city_seq                                              as project_city_seq,-- 20191015 

if(from_unixtime(t1.create_datetime,'yyyyMMdd')>='20180101' and t14.adjust_city_id is not null,t15.region,t8.region)             as emp_region,--20210316
if(from_unixtime(t1.create_datetime,'yyyyMMdd')>='20180101' and t14.adjust_city_id is not null,t15.mgr_city_id,t8.mgr_city_id)   as emp_mgr_city_id, --20210316
if(from_unixtime(t1.create_datetime,'yyyyMMdd')>='20180101' and t14.adjust_city_id is not null,t15.mgr_city,t8.mgr_city)         as emp_mgr_city,--20210316
if(from_unixtime(t1.create_datetime,'yyyyMMdd')>='20180101' and t14.adjust_city_id is not null,t14.adjust_city_id,t7.city_id)    as emp_city_id,--20210316
if(from_unixtime(t1.create_datetime,'yyyyMMdd')>='20180101' and t14.adjust_city_id is not null,t15.city_name,t7.city_name)       as emp_city_name,--20210316
if(from_unixtime(t1.create_datetime,'yyyyMMdd')>='20180101' and t14.adjust_city_id is not null,t15.city_seq,t8.city_seq)         as emp_city_seq,--20210316

t2.project_id                                            as project_id,
coalesce(t2.project_name,t7.project_name)                as project_name, 

t1.employee_id                                           as clue_emp_id,
t1.employee_realname                                     as clue_emp_realname,
t1.see_employee_id                                       as see_emp_id,
t1.see_employee_realname                                 as see_emp_realname,
t1.invitation_employee_id                                as invitation_emp_id,
t1.invitation_employee_realname                          as invitation_emp_realname,
t2.didi_employee_id                                      as didi_emp_id,
t2.didi_employee_id                                      as didi_emp_name,
t1.user_id                                               as user_id,
t1.user_realname                                         as user_name,

from_unixtime(t1.plan_real_begin_datetime)               as plan_real_begin_time,
from_unixtime(t1.plan_to_real_datetime)                  as plan_to_real_time,
from_unixtime(t1.plan_real_end_datetime)                 as plan_real_end_time,

t1.status                                                as status,
t1.merg_tag                                              as merg_tag,
t1.see_project_type                                      as see_project_type,
t1.audit_status                                          as audit_status,
t1.follow_tags                                           as follow_tags,
t1.sure_method                                           as sure_method,
t1.is_first_visit                                        as is_first_visit,
t1.see_type                                              as see_type,
t1.have_confirmation_sheet                               as have_confirmation_sheet,
t5.is_first_see                                          as is_first_see,
t6.is_first_see_project                                  as is_first_see_project,

if(row_number()over(partition by t1.id,t7.city_id) = 1,1,0)                                                              as orig_see_num, -- 带看指标 
if(t2.id is null,0,1)                                                                                                    as orig_see_project_num, -- 带看楼盘指标 
if(row_number()over(partition by t1.id,t7.city_id) = 1 and t1.status >= 40 and t1.status < 60 and t1.id is not null,1,0) as see_num, -- 有效带看量 
if(t2.id is not null and t1.status >= 40 and t1.status < 60,1,0)                                                         as see_project_num, -- 有效带看楼盘量

-- if(t10.id is null and t11.id is null,1,
--    if(t10.id is not null,2,
--       if(t11.id is not null,3,999)))                     as from_source,

case 
-- when t4.from_source is not null then t4.from_source 
when t10.id is not null then 2 -- 乌鲁木齐数据 
when t11.id is not null then 3 -- 二手房中介项目数据 
when (t12.org_type != 1 and t12.join_type not in (0,1,2) and t12.id is not null) then 4 -- 加盟商数据 
else 1 -- 居理自营数据 
end                                                      as from_source, -- 数据来源 

from_unixtime(t1.create_datetime)                        as see_create_time,
from_unixtime(t2.create_datetime)                        as create_time,
current_timestamp()                                      as etl_time


-- ETL逻辑 ---------------------------------------------------------------------------------------------------------------------------
from ods.yw_see_project t1 
full join ods.yw_see_project_list t2 on t1.id = t2.see_project_id 
left join julive_dim.dim_city t3 on t1.city_id = t3.city_id 
left join julive_dim.dim_clue_base_info t4 on t1.order_id = t4.clue_id and t4.from_source != 31 
left join ( -- 订单 首复访计算 

select 

t.id,
if(row_number()over(partition by t.order_id order by t.plan_real_begin_datetime asc) = 1,1,0) as is_first_see

from ods.yw_see_project t 
where t.status >= 40 
and t.status < 60 
and t.employee_realname != '咨询师测试'
and t.merg_tag = 0 

) t5 on t1.id = t5.id 
left join ( -- 订单-楼盘 首复访计算 

select 

a2.id,
if(row_number()over(partition by a1.order_id,a2.project_id order by a1.plan_real_begin_datetime asc) = 1,1,0) as is_first_see_project

from ods.yw_see_project  a1 
full join ods.yw_see_project_list a2 on a1.id = a2.see_project_id 
where a1.status >= 40 
and a1.status < 60 
and a1.employee_realname != '咨询师测试'
and a1.merg_tag = 0 

) t6 on t2.id = t6.id 
left join julive_dim.dim_project_info t7 on t2.project_id = t7.project_id and t7.end_date = '9999-12-31' 
left join julive_dim.dim_city t8 on t7.city_id = t8.city_id 
left join julive_dim.dim_wlmq_city t9 on t7.city_id = t9.city_id
left join ods.yw_developer_city_config t10 on t7.city_id = t10.city_id -- 乌鲁木齐数据 
left join ods.yw_esf_virtual_config t11 on t7.city_id = t11.virtual_city -- 二手房中介 
left join ods.yw_org_info t12 on t1.org_id = t12.org_id -- 加盟商数据 
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
) t13  on t1.org_id = t13.department_id
left join julive_dim.dim_employee_base_info t14 on t1.see_employee_id =t14.emp_id and regexp_replace(to_date(from_unixtime(t1.create_datetime)),'-','') = t14.pdate
left join julive_dim.dim_city t15 on t14.adjust_city_id =t15.city_id

;


-- ------------------------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------------------------
-- 加工子表 


-- 居理自营数据 
insert overwrite table julive_fact.fact_see_project_dtl
select t.* 
from julive_fact.fact_see_project_base_dtl t 
where t.from_source = 1 
;
-- 乌鲁木齐数据 
insert overwrite table julive_fact.fact_wlmq_see_project_dtl 
select t.* 
from julive_fact.fact_see_project_base_dtl t 
where t.from_source = 2 
;
-- 二手房中介数据 
insert overwrite table julive_fact.fact_esf_see_project_dtl
select t.* 
from julive_fact.fact_see_project_base_dtl t 
where t.from_source = 3 
;
-- 加盟商数据 
insert overwrite table julive_fact.fact_jms_see_project_dtl
select t.* 
from julive_fact.fact_see_project_base_dtl t 
where t.from_source = 4  
;
insert into table julive_fact.fact_jms_see_project_dtl
select t.* 
from julive_fact.fact_see_project_base_dtl t 
where t.from_source = 1 and t.org_id != 48  
;

