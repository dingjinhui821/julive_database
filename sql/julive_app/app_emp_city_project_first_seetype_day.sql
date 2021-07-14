set mapred.job.name=fact_grass_sign_dtl;
set mapreduce.job.queuename=root.etl;

with t1 as 
(
select 

tmp.date_str,
tmp.city_id,
tmp.is_first_see_project,
tmp.emp_id,
tmp.project_id,
tmp.project_name,
tmp.from_source,
tmp.org_id,
tmp.org_name,
sum(tmp.see_project_num) as see_project_num,
sum(tmp.subscribe_contains_cancel_ext_num) as subscribe_contains_cancel_ext_num,
sum(tmp.subscribe_contains_cancel_ext_income) as subscribe_contains_cancel_ext_income

from (

select 

to_date(t.plan_real_begin_time)      as date_str,
t.project_city_id                    as city_id,
if(t.is_first_see_project = 1,'首访','复访') as is_first_see_project,
t.see_emp_id                         as emp_id,
t.project_id                         as project_id,
t.project_name                       as project_name,
-- 20210317增加来源，公司id名称
case
when t.from_source is not null then t.from_source
else 1 end                           as from_source,
t.org_id                             as org_id,
t.org_name                           as org_name,
sum(t.see_project_num)               as see_project_num,
0                                    as subscribe_contains_cancel_ext_num,
0                                    as subscribe_contains_cancel_ext_income

from julive_fact.fact_see_project_base_dtl t 
where t.status >= 40 
and t.status < 60 
group by 
to_date(t.plan_real_begin_time),
t.project_city_id,
t.is_first_see_project,
t.see_emp_id,
t.project_id,
t.project_name,
case
when t.from_source is not null then t.from_source
else 1 end,
t.org_id,
t.org_name

union all 

select 

to_date(t.subscribe_time)                as date_str,
t.project_city_id                        as city_id,
if(t.is_first_see_project = 1,'首访','复访') as is_first_see_project,
t.emp_id                                 as emp_id,
t.project_id                             as project_id,
t.project_name                           as project_name,
-- 20210317增加来源，公司id名称
case
when t.from_source is not null then t.from_source
else 1 end                               as from_source,
t.org_id                                 as org_id,
t.org_name                               as org_name,
0                                        as see_project_num,
sum(t.subscribe_contains_cancel_ext_num) as subscribe_contains_cancel_ext_num,
sum(t.subscribe_contains_cancel_ext_income) as subscribe_contains_cancel_ext_income

from julive_fact.fact_subscribe_base_dtl t 
group by 
to_date(t.subscribe_time),
t.project_city_id,
t.is_first_see_project,
t.emp_id,
t.project_id,
t.project_name,
case
when t.from_source is not null then t.from_source
else 1 end,
t.org_id,
t.org_name

) tmp  
group by

tmp.date_str,
tmp.city_id,
tmp.is_first_see_project,
tmp.emp_id,
tmp.project_id,
tmp.project_name,
tmp.from_source,
tmp.org_id,
tmp.org_name
),

t7 as 

(
select 

tmp.date_str,
tmp.is_first_see_project,
tmp.emp_id,
tmp.project_id,
tmp.project_name,
tmp.from_source,
tmp.org_id,
tmp.org_name,
tmp.emp_city_id,
tmp.emp_city_name,
tmp.emp_city_seq,
sum(tmp.emp_city_see_project_num) as emp_city_see_project_num,
sum(tmp.emp_city_subscribe_contains_cancel_ext_num) as emp_city_subscribe_contains_cancel_ext_num,
sum(tmp.emp_city_subscribe_contains_cancel_ext_income) as emp_city_subscribe_contains_cancel_ext_income

from (

select 

to_date(t.plan_real_begin_time)      as date_str,
if(t.is_first_see_project = 1,'首访','复访') as is_first_see_project,
t.see_emp_id                         as emp_id,
t.project_id                         as project_id,
t.project_name                       as project_name,
-- 20210317增加来源，公司id名称
case
when t.from_source is not null then t.from_source
else 1 end                           as from_source,
t.org_id                             as org_id,
t.org_name                           as org_name,
-- 20210322 增加雇员信息
t.emp_city_id                        as emp_city_id,
t.emp_city_name                      as emp_city_name,
t.emp_city_seq                       as emp_city_seq,
sum(t.see_project_num)               as emp_city_see_project_num,
0                                    as emp_city_subscribe_contains_cancel_ext_num,
0                                    as emp_city_subscribe_contains_cancel_ext_income

from julive_fact.fact_see_project_base_dtl t 
where t.status >= 40 
and t.status < 60 
group by 
to_date(t.plan_real_begin_time),
t.is_first_see_project,
t.see_emp_id,
t.project_id,
t.project_name,
case
when t.from_source is not null then t.from_source
else 1 end,
t.org_id,
t.org_name,
t.emp_city_id,
t.emp_city_name,
t.emp_city_seq

union all 

select 

to_date(t.subscribe_time)                as date_str,
if(t.is_first_see_project = 1,'首访','复访') as is_first_see_project,
t.emp_id                                 as emp_id,
t.project_id                             as project_id,
t.project_name                           as project_name,
-- 20210317增加来源，公司id名称
case
when t.from_source is not null then t.from_source
else 1 end                               as from_source,
t.org_id                                 as org_id,
t.org_name                               as org_name,
-- 20210322 增加雇员信息
t.emp_city_id                        as emp_city_id,
t.emp_city_name                      as emp_city_name,
t.emp_city_seq                       as emp_city_seq,
0                                        as emp_city_see_project_num,
sum(t.subscribe_contains_cancel_ext_num) as emp_city_subscribe_contains_cancel_ext_num,
sum(t.subscribe_contains_cancel_ext_income) as emp_city_subscribe_contains_cancel_ext_income

from julive_fact.fact_subscribe_base_dtl t 
group by 
to_date(t.subscribe_time),
t.project_city_id,
t.is_first_see_project,
t.emp_id,
t.project_id,
t.project_name,
case
when t.from_source is not null then t.from_source
else 1 end,
t.org_id,
t.org_name,
t.emp_city_id,
t.emp_city_name,
t.emp_city_seq

) tmp  
group by

tmp.date_str,
tmp.is_first_see_project,
tmp.emp_id,
tmp.project_id,
tmp.project_name,
tmp.from_source,
tmp.org_id,
tmp.org_name,
tmp.emp_city_id,
tmp.emp_city_name,
tmp.emp_city_seq
),

t8 as (

select 
t1.date_str,
t1.city_id,
t1.is_first_see_project,
t1.emp_id,
t1.project_id,
t1.project_name,
t1.from_source,
t1.org_id,
t1.org_name,
t1.see_project_num,
t1.subscribe_contains_cancel_ext_num,
t1.subscribe_contains_cancel_ext_income,
t7.emp_city_id,
t7.emp_city_name,
t7.emp_city_seq,
t7.emp_city_see_project_num,
t7.emp_city_subscribe_contains_cancel_ext_num,
t7.emp_city_subscribe_contains_cancel_ext_income
from t1 
full join t7
on t1.date_str = t7.date_str 
and t1.is_first_see_project = t7.is_first_see_project 
and t1.emp_id = t7.emp_id
and t1.project_id = t7.project_id 
and t1.from_source = t7.from_source
and t1.org_id = t7.org_id
)

insert overwrite table julive_app.app_emp_city_project_first_seetype_day_base 
select 
t8.date_str,
t5.week_type,
t8.city_id,
t4.city_name,
t4.city_seq,
t4.region as city_region,
t4.city_type_first as city_type,
t4.mgr_city,
t8.emp_id,
t3.emp_name,
t8.project_id,
t8.project_name,
t6.project_type,
t6.district_id,
t6.district_name,
t8.is_first_see_project,

t2.direct_leader_id,
t2.direct_leader_name,
t2.indirect_leader_id,
t2.indirect_leader_name,

t3.direct_leader_id      as now_direct_leader_id,
t3.direct_leader_name    as now_direct_leader_name,
t3.indirect_leader_id    as now_indirect_leader_id,
t3.indirect_leader_name  as now_indirect_leader_name,

t3.full_type,
t3.full_type_tc,

t8.see_project_num,
t8.subscribe_contains_cancel_ext_num,
t8.subscribe_contains_cancel_ext_income,
-- 20210317新增字段
t8.from_source,
t8.org_id,
t8.org_name,
-- 20210322新增字段
t8.emp_city_id,
t8.emp_city_name,
t8.emp_city_seq,
t8.emp_city_see_project_num,
t8.emp_city_subscribe_contains_cancel_ext_num,
t8.emp_city_subscribe_contains_cancel_ext_income,
current_timestamp() as etl_time 
from t8
-- 20210317 dim_ps_employee_info替换成dim_employee_base_info 增加限制条件
left join julive_dim.dim_employee_base_info t2 on t8.emp_id = t2.emp_id and regexp_replace(t8.date_str,'-','') = t2.pdate 
-- 20210317 dim_employee_info替换成dim_employee_base_info 增加限制条件
left join julive_dim.dim_employee_base_info t3 on t8.emp_id = t3.emp_id and t3.end_date = '9999-12-31' 
left join julive_dim.dim_city t4 on t8.city_id = t4.city_id 
left join julive_dim.dim_date t5 on t8.date_str = t5.date_str 
left join julive_dim.dim_project_info t6 on t8.project_id = t6.project_id and t6.end_date = '9999-12-31' 
;


-- 加工子表 


-- 居理数据 
insert overwrite table julive_app.app_emp_city_project_first_seetype_day 
select t.* 
from julive_app.app_emp_city_project_first_seetype_day_base t 
where t.from_source = 1 
;
-- 乌鲁木齐数据 
insert overwrite table julive_app.app_wlmq_emp_city_project_first_seetype_day 
select t.* 
from julive_app.app_emp_city_project_first_seetype_day_base t 
where t.from_source = 2 
;
-- 二手房中介数据 
insert overwrite table julive_app.app_esf_emp_city_project_first_seetype_day
select t.* 
from julive_app.app_emp_city_project_first_seetype_day_base t 
where t.from_source = 3 
;
-- 加盟商数据 
insert overwrite table julive_app.app_jms_emp_city_project_first_seetype_day 
select t.* 
from julive_app.app_emp_city_project_first_seetype_day_base t 
where t.from_source = 4 
;

insert into table julive_app.app_jms_emp_city_project_first_seetype_day 
select t.* 
from julive_app.app_emp_city_project_first_seetype_day_base t 
where t.from_source = 1 and t.org_id!=48 
;

