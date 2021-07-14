set hive.execution.engine=spark;
set spark.app.name=fact_zxs_adjust_cust_see_dtl;
set spark.yarn.queue=etl;
insert overwrite table julive_fact.fact_zxs_adjust_cust_see_base_dtl 

select 

t1.see_project_id                                   as see_id,
t2.org_id                                           as org_id,
t13.org_type                                        as org_type,
t14.team_name                                       as org_name,
t1.employee_id                                      as emp_id,
t3.employee_name                                    as emp_name,
t1.manager_id                                       as emp_mgr_id,
t4.employee_name                                    as emp_mgr_name,
t1.manager_leader_id                                as emp_mgr_leader_id,
t5.employee_name                                    as emp_mgr_leader_name,

t1.city_id                                          as clue_city_id,
if(t6.city_name is not null,t6.city_name,t11.city_name) as clue_city_name,
if(t6.city_seq is not null,t6.city_seq,t11.city_seq)    as clue_city_seq,

t1.employee_adjust_city                             as adjust_city_id,
t7.city_name                                        as adjust_city_name,
t7.city_seq                                         as adjust_city_seq,

t1.manager_adjust_city                              as mgr_adjust_city_id,
t8.city_name                                        as mgr_adjust_city_name,
t8.city_seq                                         as mgr_adjust_city_seq,

t1.manager_leader_adjust_city                       as mgr_leader_adjust_city_id,
t9.city_name                                        as mgr_leader_adjust_city_name,
t9.city_seq                                         as mgr_leader_adjust_city_seq,

t1.value                                            as adjust_see_num,
to_date(from_unixtime(t2.plan_real_begin_datetime)) as plan_real_begin_date,
to_date(from_unixtime(t1.create_datetime))          as create_date,
to_date(from_unixtime(t1.happen_updatetime))        as happen_date,
case 
when t10.id is not null then 2 -- 乌鲁木齐数据 
when t12.id is not null then 3 -- 二手房中介数据 
when (t13.org_type != 1 and t13.join_type not in (0,1,2) and t13.id is not null) then 4 -- 加盟商数据 
else 1 -- 居理数据 
end                                                                             as from_source, -- 数据来源

--if(t10.id is null and t12.id is null,1,
--   if(t10.id is not null,2,
--      if(t12.id is not null,3,999)))                as from_source,
current_timestamp()                                 as etl_time 

from ods.adjust_see_project_employee_detail t1 
     join ods.yw_see_project  t2 on t1.see_project_id = t2.id 
left join ods.yw_employee t3 on t1.employee_id = t3.id 
left join ods.yw_employee t4 on t1.manager_id = t4.id 
left join ods.yw_employee t5 on t1.manager_leader_id = t5.id 
left join julive_dim.dim_city t6 on t1.city_id = t6.city_id 
left join julive_dim.dim_city t7 on t1.employee_adjust_city = t7.city_id 
left join julive_dim.dim_city t8 on t1.manager_adjust_city = t8.city_id 
left join julive_dim.dim_city t9 on t1.manager_leader_adjust_city = t9.city_id 
left join ods.yw_developer_city_config t10 on t1.city_id = t10.city_id
left join julive_dim.dim_wlmq_city t11 on t11.city_id = t1.city_id
left join ods.yw_esf_virtual_config t12 on t12.virtual_city = t1.city_id
left join ods.yw_org_info t13 on t2.org_id = t13.org_id -- 加盟商 ：20201015集成加盟商数据 
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
) t14 on t2.org_id = t14.department_id

where t2.status >= 40 
and t2.status < 60 
;

insert overwrite table julive_fact.fact_zxs_adjust_cust_see_dtl
select * from julive_fact.fact_zxs_adjust_cust_see_base_dtl
where from_source=1;

insert overwrite table julive_fact.fact_wlmq_zxs_adjust_cust_see_dtl
select * from julive_fact.fact_zxs_adjust_cust_see_base_dtl
where from_source=2;

insert overwrite table julive_fact.fact_esf_zxs_adjust_cust_see_dtl
select * from julive_fact.fact_zxs_adjust_cust_see_base_dtl
where from_source=3;

insert overwrite table julive_fact.fact_jms_zxs_adjust_cust_see_dtl
select * from julive_fact.fact_zxs_adjust_cust_see_base_dtl
where from_source=4;

insert into table julive_fact.fact_jms_zxs_adjust_cust_see_dtl
select * from julive_fact.fact_zxs_adjust_cust_see_base_dtl
where from_source=1 and org_id !=48;


