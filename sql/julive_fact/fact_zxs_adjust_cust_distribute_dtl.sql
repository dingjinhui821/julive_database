set hive.execution.engine=spark;
set spark.app.name=fact_zxs_adjust_cust_distribute_dtl;
set spark.yarn.queue=etl;

insert overwrite table julive_fact.fact_zxs_adjust_cust_distribute_base_dtl 

select 

t1.order_id                                                                                   as clue_id,
t2.org_id                                                                                     as org_id,
t2.org_type                                                                                   as org_type,
t2.org_name                                                                                   as org_name,
t1.employee_id                                                                                as emp_id,
t5.employee_name                                                                              as emp_name,
t1.manager_id                                                                                 as emp_mgr_id,
t6.employee_name                                                                              as emp_mgr_name,
t1.city_id                                                                                    as clue_city_id,
t2.city_name                                                                                  as clue_city_name,
t2.city_seq                                                                                   as clue_city_seq, 
t2.customer_intent_city_id                                                                    as customer_intent_city_id,
t2.customer_intent_city_name                                                                  as customer_intent_city_name,
t2.customer_intent_city_seq                                                                   as customer_intent_city_seq,
t1.employee_adjust_city                                                                       as adjust_city_id,
if(t3.city_name is not null ,t3.city_name,t2.city_name)                                       as adjust_city_name,
if(t3.city_seq  is not null ,t3.city_seq,t2.city_seq)                                         as adjust_city_seq, 
t1.manager_adjust_city                                                                        as mgr_adjust_city_id,
if(t3.city_name is not null,t3.city_name,t2.city_name)                                        as mgr_adjust_city_name,
if(t3.city_seq  is not null,t3.city_seq ,t2.city_seq)                                         as mgr_adjust_city_seq,
t1.value                                                                                      as adjust_distribute_num,
if(row_number()over(partition by t1.order_id order by t1.create_datetime asc) = 1,t1.value,0) as first_adjust_distribute_num,
to_date(from_unixtime(t1.create_datetime))                                                    as create_date,
to_date(from_unixtime(t1.happen_updatetime))                                                  as happen_date,
t2.from_source                                                                                as from_source,
current_timestamp()                                                                           as etl_time 

from ods.adjust_incostomer_employee_detail t1 
join julive_dim.dim_clue_base_info t2 on t1.order_id = t2.clue_id 
left join julive_dim.dim_city t3 on t1.employee_adjust_city = t3.city_id 
left join julive_dim.dim_city t4 on t1.manager_adjust_city = t4.city_id 
left join ods.yw_employee t5 on t1.employee_id = t5.id 
left join ods.yw_employee t6 on t1.manager_id = t6.id 


where t2.is_distribute = 1 
;

insert overwrite table julive_fact.fact_zxs_adjust_cust_distribute_dtl
select * from julive_fact.fact_zxs_adjust_cust_distribute_base_dtl
where from_source =1;
insert overwrite table julive_fact.fact_wlmq_zxs_adjust_cust_distribute_dtl
select * from julive_fact.fact_zxs_adjust_cust_distribute_base_dtl
where from_source =2;
insert overwrite table julive_fact.fact_esf_zxs_adjust_cust_distribute_dtl
select * from julive_fact.fact_zxs_adjust_cust_distribute_base_dtl
where from_source =3;
insert overwrite table julive_fact.fact_jms_zxs_adjust_cust_distribute_dtl
select * from julive_fact.fact_zxs_adjust_cust_distribute_base_dtl
where from_source =4;
insert into table julive_fact.fact_jms_zxs_adjust_cust_distribute_dtl
select * from julive_fact.fact_zxs_adjust_cust_distribute_base_dtl
where from_source =1 and org_id !=48;

