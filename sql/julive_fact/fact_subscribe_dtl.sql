
-- ---------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------
set hive.execution.engine=spark;
set spark.app.name=fact_subscribe_base_dtl;
set mapred.job.name=fact_subscribe_base_dtl;
set spark.yarn.queue=etl;

insert overwrite table julive_fact.fact_subscribe_base_dtl 

select 

t1.id                                                    as subscribe_id,
t1.order_id                                              as clue_id,
t1.see_project_id                                        as see_id,
t1.org_id                                                as org_id,
t12.org_type                                             as org_type,
t13.team_name                                            as org_name,
t4.channel_id                                            as channel_id,
t1.deal_id                                               as deal_id,
t1.project_id                                            as project_id,
coalesce(t1.project_name,t7.project_name)                as project_name,
t1.employee_id                                           as emp_id,
t14.emp_name                                              as emp_name,
t1.user_id                                               as user_id,
t4.user_name                                             as user_name,
t8.region                                                as region,
t8.mgr_city_seq                                          as mgr_city_seq,
t8.mgr_city                                              as mgr_city,
t1.city_id                                               as city_id,
if(t3.city_name is not null,t3.city_name,t10.city_name)  as city_name,
if(t3.city_seq is not null,t3.city_seq,t10.city_seq)     as city_seq,
t4.customer_intent_city_id                               as customer_intent_city_id,-- 20191015 
t4.customer_intent_city_name                             as customer_intent_city_name,-- 20191015 
t4.customer_intent_city_seq                              as customer_intent_city_seq,-- 20191015 


t4.source                                                as source,
t4.source_tc                                             as source_tc,
t7.city_id                                               as project_city_id,-- 20191015 
t7.city_name                                             as project_city_name,-- 20191015 
t8.city_seq                                              as project_city_seq,

if(from_unixtime(t1.subscribe_datetime,'yyyyMMdd')>='20180101' and t14.adjust_city_id is not null,t15.region,t8.region)             as emp_region,--20210316
if(from_unixtime(t1.subscribe_datetime,'yyyyMMdd')>='20180101' and t14.adjust_city_id is not null,t15.mgr_city_seq,t8.mgr_city_seq) as emp_mgr_city_seq, --20210316
if(from_unixtime(t1.subscribe_datetime,'yyyyMMdd')>='20180101' and t14.adjust_city_id is not null,t15.mgr_city,t8.mgr_city)         as emp_mgr_city,--20210316
if(from_unixtime(t1.subscribe_datetime,'yyyyMMdd')>='20180101' and t14.adjust_city_id is not null,t14.adjust_city_id,t7.city_id)    as emp_city_id,--20210316
if(from_unixtime(t1.subscribe_datetime,'yyyyMMdd')>='20180101' and t14.adjust_city_id is not null,t15.city_name,t7.city_name)       as emp_city_name,--20210316
if(from_unixtime(t1.subscribe_datetime,'yyyyMMdd')>='20180101' and t14.adjust_city_id is not null,t15.city_seq,t8.city_seq)         as emp_city_seq,--20210316

t1.status                                                as subscribe_status,
t1.subscribe_type                                        as subscribe_type,

if(t5.is_second = 1 or (t5.is_second = 2 and t1.subscribe_datetime >= t5.first_time and t1.subscribe_datetime < t5.second_time),1,0) as is_first_see,
if(t6.is_second = 1 or (t6.is_second = 2 and t1.subscribe_datetime >= t6.first_time and t1.subscribe_datetime < t6.second_time),1,0) as is_first_see_project,

-- 原始指标 
t1.deal_money                                     as orig_deal_amt,
t2.forecast_payment_total                         as orig_subsctibe_income, 
if(t1.status = 1,1,0)                             as orig_subscribe_num, --认购指标
if(t1.status in (1,2),1,0)                        as orig_subscribe_contains_cancel_num, --认购含退指标

-- 金额 
if(t1.status in (1,2) and t1.subscribe_type in (1,4),t1.deal_money,0) as subscribe_contains_cancel_ext_amt,
if(t1.status in (1,2) and t1.subscribe_type in (1,4),t2.forecast_payment_total,0) as subscribe_contains_cancel_ext_income,
if(t1.status = 1 and t1.subscribe_type in (1,4),t1.deal_money,0) as subscribe_contains_ext_amt,
if(t1.status = 1 and t1.subscribe_type in (1,4),t2.forecast_payment_total,0) as subscribe_contains_ext_income,
if(t1.status = 1 and t1.subscribe_type in (1),t1.deal_money,0) as subscribe_coop_amt,
if(t1.status = 1 and t1.subscribe_type in (1),t2.forecast_payment_total,0) as subscribe_coop_income,
if(t1.status = 2 and t1.subscribe_type in (1,4),t1.deal_money,0) as subscribe_cancel_contains_ext_amt,
if(t1.status = 2 and t1.subscribe_type in (1,4),t2.forecast_payment_total,0) as subscribe_cancel_contains_ext_income,
if(t1.status = 2 and t1.subscribe_type in (1),t1.deal_money,0) as subscribe_cancel_coop_amt,

-- 量 
if(t1.status in (1,2) and t1.subscribe_type in (1,4),1,0) as subscribe_contains_cancel_ext_num,
if(t1.status in (1,2) and t1.subscribe_type in (1),1,0) as subscribe_contains_cancel_num,
if(t1.status = 1 and t1.subscribe_type in (1),1,0) as subscribe_coop_num,
if(t1.status = 1 and t1.subscribe_type in (1,4),1,0) as subscribe_contains_ext_num,
if(t1.status = 2 and t1.subscribe_type in (1),1,0) as subscribe_cancel_coop_num,
if(t1.status = 2 and t1.subscribe_type in (1,4),1,0) as subscribe_cancel_contains_ext_num,

from_unixtime(t1.subscribe_datetime)              as subscribe_time,
from_unixtime(t2.back_datetime)                   as back_time,

-- if(t9.id is null and t11.id is null,1,
--    if(t9.id is not null,2,
--       if(t11.id is not null,3,999)))              as from_source,

case 
when t9.id is not null then 2 -- 乌鲁木齐数据 
when t11.id is not null then 3 -- 二手房数据 
when (t12.org_type != 1 and t12.join_type not in (0,1,2) and t12.org_type is not null) then 4 -- 加盟商数据 
else 1 -- 居理自营数据 
end                                               as from_source,

from_unixtime(t1.create_datetime)                 as create_time,
current_timestamp()                               as etl_time 


from ods.yw_subscribe t1 
left join ods.yw_deal t2 on t1.deal_id = t2.id 
left join julive_dim.dim_city t3 on t1.city_id = t3.city_id 
left join julive_dim.dim_clue_base_info t4 on t1.order_id = t4.clue_id -- order_id 是否唯一？
left join ( -- 首付访 认购 

select 

t.order_id,
min(if(t.rn = 1,t.plan_real_begin_datetime,0)) as first_time,
max(if(t.rn = 2,t.plan_real_begin_datetime,0)) as second_time,
count(1) as is_second 

from (
select 

order_id,
plan_real_begin_datetime,
row_number()over(partition by order_id order by plan_real_begin_datetime asc) as rn 

from ods.yw_see_project 
where status >= 40 
  and status < 60 
) t 
where t.rn <= 2 
group by t.order_id 

) t5 on t1.order_id = t5.order_id 
left join ( -- 订单-楼盘 首复访 

select 

t.order_id,
t.project_id, 
min(if(t.rn = 1,t.plan_real_begin_datetime,0)) as first_time,
max(if(t.rn = 2,t.plan_real_begin_datetime,0)) as second_time,
count(1) as is_second 

from (

select 

a1.order_id,
a2.project_id,
a1.plan_real_begin_datetime,
row_number()over(partition by a1.order_id,a2.project_id order by a1.plan_real_begin_datetime asc) as rn 

from ods.yw_see_project a1 
join ods.yw_see_project_list a2 on a1.id = a2.see_project_id 
where a1.status >= 40 
and a1.status < 60 
and a1.employee_realname != '咨询师测试'
and a1.merg_tag = 0 

) t 
where t.rn <= 2 
group by 
t.order_id,
t.project_id  

) t6 on t1.order_id = t6.order_id and t2.project_id = t6.project_id 
left join julive_dim.dim_project_info t7 on t1.project_id = t7.project_id and t7.end_date = '9999-12-31' 
left join julive_dim.dim_city t8 on t7.city_id = t8.city_id 
left join ods.yw_developer_city_config t9 on t7.city_id = t9.city_id -- 乌鲁木齐数据 
left join julive_dim.dim_wlmq_city t10 on t10.city_id = t7.city_id 
left join ods.yw_esf_virtual_config t11 on t11.virtual_city = t7.city_id -- 二手房数据 
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
) t13 on t1.org_id = t13.department_id
left join julive_dim.dim_employee_base_info t14 on t1.employee_id =t14.emp_id and regexp_replace(to_date(from_unixtime(t1.subscribe_datetime)),'-','') = t14.pdate
left join julive_dim.dim_city t15 on t14.adjust_city_id =t15.city_id

where t1.status != -1 

;


-- ---------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------
-- 产出子表
-- 自营数据 
insert overwrite table julive_fact.fact_subscribe_dtl
select t.* 
from julive_fact.fact_subscribe_base_dtl t 
where t.from_source = 1 ;

-- 乌鲁木齐 
insert overwrite table julive_fact.fact_wlmq_subscribe_dtl
select t.* 
from julive_fact.fact_subscribe_base_dtl t 
where t.from_source = 2 ;

-- 二手房中介数据 
insert overwrite table julive_fact.fact_esf_subscribe_dtl
select t.*  
from julive_fact.fact_subscribe_base_dtl t 
where t.from_source = 3 ;

-- 加盟商数据 
insert overwrite table julive_fact.fact_jms_subscribe_dtl
select t.*  
from julive_fact.fact_subscribe_base_dtl t 
where t.from_source = 4 ;

-- 内部加盟商数据
insert into table julive_fact.fact_jms_subscribe_dtl
select t.*  
from julive_fact.fact_subscribe_base_dtl t 
where t.from_source = 1 and t.org_id!=48
;



