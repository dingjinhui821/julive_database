set hive.execution.engine=spark;
set spark.app.name=fact_employee_check_dtl;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024; 
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table julive_fact.fact_employee_check_base_dtl 

select 

t1.id                                                                     as emp_check_id,
t1.employee_id                                                            as emp_id,
t2.emp_name                                                               as emp_name,
t2.job_number                                                             as job_number,
t2.post_id                                                                as post_id,
t2.post_name                                                              as post_name,
t2.entry_date                                                             as entry_date,
t2.dept_id                                                                as dept_id,
t2.dept_name                                                              as dept_name,
t2.adjust_city_id                                                         as city_id,
if(t4.city_name is not null,t4.city_name,t6.city_name)                    as city_name,
if(t4.city_seq is not null,t4.city_seq,t6.city_seq)                       as city_seq,

from_unixtime(t1.check_date,'yyyy-MM-dd')                                 as check_date,
t3.weekday_zh                                                             as weekday_zh,
from_unixtime(t1.work_check_time,'yyyy-MM-dd HH:mm:ss')                   as work_check_time,
from_unixtime(t1.leave_check_time,'yyyy-MM-dd HH:mm:ss')                  as leave_check_time,
from_unixtime(t1.plan_work_time,'yyyy-MM-dd HH:mm:ss')                    as plan_work_time,
from_unixtime(t1.plan_leave_time,'yyyy-MM-dd HH:mm:ss')                   as plan_leave_time,
t1.status                                                                 as status,

if(from_unixtime(t1.plan_work_time,'yyyy-MM-dd') = '1970-01-01',0,1)      as plan_workday_num,

if(t1.over_work_days = 0,
if(if(t1.work_check_time = 0,0,from_unixtime(t1.work_check_time)) = 0 
and if(t1.leave_check_time=0,0,from_unixtime(t1.leave_check_time)) = 0,0,1) - t1.personal_leave_days,
t1.over_work_days)                                                        as real_workday_num,
if(t5.id is null and t7.id is null,1,
   if(t5.id is not null,2,
      if(t7.id is not null,3,999)))                                       as from_source,

current_timestamp()                                                       as etl_time 

from  
 ods.hr_employee_check   t1 
left join julive_dim.dim_ps_employee_info t2 on t1.employee_id = t2.emp_id and from_unixtime(t1.check_date,'yyyyMMdd') = t2.pdate 
left join julive_dim.dim_date t3 on from_unixtime(t1.check_date,'yyyyMMdd') = t3.skey 
left join julive_dim.dim_city t4 on t2.adjust_city_id = t4.city_id 
left join ods.yw_developer_city_config t5 on t5.city_id = t2.adjust_city_id
left join julive_dim.dim_wlmq_city t6 on t6.city_id = t2.adjust_city_id
left join ods.yw_esf_virtual_config t7 on t7.virtual_city = t2.adjust_city_id

where t1.employee_id != -1 
and t1.post_id = 163 
and from_unixtime(t1.check_date,'yyyy-MM-dd') >= '2018-01-01' 
and abs(datediff(to_date(t2.entry_date),from_unixtime(t1.check_date,'yyyy-MM-dd'))) > 15 
;

insert overwrite table julive_fact.fact_employee_check_dtl 
select * from julive_fact.fact_employee_check_base_dtl 
where from_source =1;

insert overwrite table julive_fact.fact_wlmq_employee_check_dtl 
select * from julive_fact.fact_employee_check_base_dtl 
where from_source =2;

insert overwrite table julive_fact.fact_esf_employee_check_dtl 
select * from julive_fact.fact_employee_check_base_dtl 
where from_source =3;


insert overwrite table julive_fact.fact_jms_employee_check_dtl 
select * from julive_fact.fact_employee_check_base_dtl 
where from_source =4;

