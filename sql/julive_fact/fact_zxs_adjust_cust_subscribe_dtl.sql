set hive.execution.engine=spark;
set spark.app.name=fact_zxs_adjust_cust_subscribe_dtl;
set spark.yarn.queue=etl;
insert overwrite table julive_fact.fact_zxs_adjust_cust_subscribe_base_dtl 

select 

t1.subscribe_id                                                                              as subscribe_id,
t2.org_id                                                                                    as org_id,
t12.org_type                                                                                 as org_type,
t13.team_name                                                                                as org_name,
t1.employee_id                                                                               as emp_id,
t4.employee_name                                                                             as emp_name,
t1.manager_id                                                                                as emp_mgr_id,
t5.employee_name                                                                             as emp_mgr_name,
t1.city_id                                                                                   as clue_city_id,
if(t6.city_name is not null,t6.city_name,t10.city_name)                                      as clue_city_name,
if(t6.city_seq  is not null,t6.city_seq ,t10.city_seq )                                      as clue_city_seq,
t1.employee_adjust_city                                                                      as adjust_city_id,
t7.city_name                                                                                 as adjust_city_name, 
t7.city_seq                                                                                  as adjust_city_seq,
t1.manager_adjust_city                                                                       as mgr_adjust_city_id,
t8.city_name                                                                                 as mgr_adjust_city_name, 
t8.city_seq                                                                                  as mgr_adjust_city_seq,
t2.status                                                                                    as subscribe_status,
t2.subscribe_type                                                                            as subscribe_type,
t3.forecast_payment_total                                                                    as orig_subsctibe_income,
t1.value                                                                                     as orig_adjust_subscribe_num,
-- 指标:收入 
if(t2.status in (1,2) and t2.subscribe_type in (1,4),t3.forecast_payment_total * t1.value,0) as adjust_subscribe_contains_cancel_ext_income, -- 核算认购-含退、含外联收入(佣金)
if(t2.status in (1) and t2.subscribe_type in (1,4),t3.forecast_payment_total * t1.value,0)   as adjust_subscribe_contains_ext_income, -- 核算认购-不含退、含外联收入(佣金)
if(t2.status in (1,2) and t2.subscribe_type in (1),t3.forecast_payment_total * t1.value,0)   as adjust_subscribe_contains_cancel_income, -- 核算认购-含退、不含外联收入(佣金)
if(t2.status in (1) and t2.subscribe_type in (1),t3.forecast_payment_total * t1.value,0)     as adjust_subscribe_coop_income, -- 核算认购-合作、不含外联收入(佣金) 
-- 指标:量 
if(t2.status in (1,2) and t2.subscribe_type in (1,4),t1.value,0)                             as adjust_subscribe_contains_cancel_ext_num, -- 核算认购量-含退、含外联
if(t2.status in (1) and t2.subscribe_type in (1,4),t1.value,0)                               as adjust_subscribe_contains_ext_num, -- 核算认购量-不含退、含外联
if(t2.status in (1,2) and t2.subscribe_type in (1),t1.value,0)                               as adjust_subscribe_contains_cancel_num, -- 核算认购量-含退、不含外联
if(t2.status in (1) and t2.subscribe_type in (1),t1.value,0)                                 as adjust_subscribe_coop_num, -- 核算认购量-合作、不含外联 

to_date(from_unixtime(t2.subscribe_datetime))                                                as subscribe_date,
to_date(from_unixtime(t1.create_datetime))                                                   as create_date,
to_date(from_unixtime(t1.happen_updatetime))                                                 as happen_date,
case 
when t9.id is not null then 2 -- 乌鲁木齐数据 
when t11.id is not null then 3 -- 二手房中介数据 
when (t12.org_type != 1 and t12.join_type not in (0,1,2) and t12.id is not null) then 4 -- 加盟商数据 
else 1 -- 居理数据 
end                                                                             as from_source, -- 数据来源

--if(t9.id is null and t11.id is null,1,
--   if(t9.id is not null,2,
--      if(t11.id is not null,3,999)))                                                         as from_source,
current_timestamp()                                                                          as etl_time,
t2.order_id                                                                                  as clue_id

from ods.adjust_subscribe_employee_detail t1 
join
 ods.yw_subscribe  t2 on t1.subscribe_id = t2.id 
left join ods.yw_deal t3 on t2.deal_id = t3.id 

left join ods.yw_employee t4 on t1.employee_id = t4.id  
left join ods.yw_employee t5 on t1.manager_id = t5.id 

left join julive_dim.dim_city t6 on t1.city_id = t6.city_id 
left join julive_dim.dim_city t7 on t1.employee_adjust_city = t7.city_id 
left join julive_dim.dim_city t8 on t1.manager_adjust_city = t8.city_id
left join ods.yw_developer_city_config t9 on t1.city_id = t9.city_id
left join julive_dim.dim_wlmq_city t10 on t10.city_id = t1.city_id
left join ods.yw_esf_virtual_config t11 on t11.virtual_city = t1.city_id 
left join ods.yw_org_info t12 on t2.org_id = t12.org_id -- 加盟商 ：20201015集成加盟商数据 
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
) t13 on t2.org_id = t13.department_id--20201224

where t1.employee_id > 0 
and t2.status != -1 
and t2.employee_id != 20000124 
;

insert overwrite table julive_fact.fact_zxs_adjust_cust_subscribe_dtl
select * from julive_fact.fact_zxs_adjust_cust_subscribe_base_dtl
where from_source =1;
insert overwrite table julive_fact.fact_wlmq_zxs_adjust_cust_subscribe_dtl
select * from julive_fact.fact_zxs_adjust_cust_subscribe_base_dtl
where from_source =2;
insert overwrite table julive_fact.fact_esf_zxs_adjust_cust_subscribe_dtl
select * from julive_fact.fact_zxs_adjust_cust_subscribe_base_dtl
where from_source =3;
insert overwrite table julive_fact.fact_jms_zxs_adjust_cust_subscribe_dtl
select * from julive_fact.fact_zxs_adjust_cust_subscribe_base_dtl
where from_source =4;
insert into table julive_fact.fact_jms_zxs_adjust_cust_subscribe_dtl
select * from julive_fact.fact_zxs_adjust_cust_subscribe_base_dtl
where from_source =1 and org_id !=48;

