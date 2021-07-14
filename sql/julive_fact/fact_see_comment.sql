-------------------------------------------------------------------------------
-- ETL ------------------------------------------------------------------------
-------------------------------------------------------------------------------
set hive.execution.engine=spark;
set spark.app.name=fact_see_comment;
set mapred.job.name=fact_see_comment;
set spark.yarn.queue=etl;
insert overwrite table julive_fact.fact_see_comment 

select 

t2.order_id                                                                                                  as clue_id,
t1.id                                                                                                        as see_id,
t2.see_employee_id                                                                                           as see_emp_id,
t2.see_employee_name                                                                                         as see_emp_name,
t2.employee_leader_id                                                                                        as emp_leader_id,
t2.employee_leader_realname                                                                                  as emp_leader_name,

t1.city_id                                                                                                   as city_id,
t5.city_name                                                                                                 as city_name,
t5.city_seq                                                                                                  as city_seq,

t4.city_id                                                                                                   as project_city_id,-- 20191015 
t6.city_name                                                                                                 as project_city_name,-- 20191015 
t6.city_seq                                                                                                  as project_city_seq,-- 20191015 

t2.probability                                                                                               as probability, -- 20191028添加 
if(t2.employee_grade >= coalesce(t3.final_grade,0),t2.employee_grade,t3.final_grade)                         as final_grade,
if(t2.global_comment is not null and t2.global_comment != '',1,0)                                            as is_txt_comment,
length(t2.global_comment)                                                                                    as txt_comment_num, -- 20191028添加 
(1 + coalesce(t3.comment_num,0))                                                                             as comment_num,

if(t2.employee_grade >= coalesce(t3.final_grade,0),from_unixtime(t2.visit_datetime),t3.business_happen_time) as visit_time,
coalesce(from_unixtime(t2.create_datetime),t3.task_grade_time)                                               as create_time,
current_timestamp()                                                                                          as etl_time 

from ods.yw_see_project t1 
join ods.yw_see_project_global_comment t2 on t1.id = t2.see_project_id 
left join (

select 

t.see_comment_id,
t.final_grade,
t.business_happen_time,
t.task_grade_time,
t.comment_num 

from (
select 

business_id                                                          as see_comment_id,
final_grade                                                          as final_grade,
from_unixtime(business_happen_datetime)                              as business_happen_time, -- 业务发生时间
from_unixtime(task_grade_datetime)                                   as task_grade_time, -- 追回评价时间
count(1)over(partition by business_id)                               as comment_num,
row_number()over(partition by business_id order by final_grade desc) as rn 

from ods.yw_replevy 
where type = 1 -- 带看追回 
and final_grade is not null 
) t 
where t.rn = 1 

) t3 on t2.id = t3.see_comment_id 
left join (
select 

t1.id,
t3.city_id

from ods.yw_see_project t1 
join ods.yw_see_project_list t2 on t1.id = t2.see_project_id 
left join julive_dim.dim_project_info t3 on t2.project_id = t3.project_id and t3.end_date = '9999-12-31' 

group by 
t1.id,
t3.city_id
) t4 on t1.id = t4.id 
left join julive_dim.dim_city t5 on t1.city_id = t5.city_id 
left join julive_dim.dim_city t6 on t4.city_id = t6.city_id 

where t1.status >= 40 
  and t1.status < 60 
  and t1.employee_id is not null 
  and t1.see_employee_id is not null 
  --- 2020-01-16注释 and t2.status = 1 
  and t2.employee_grade is not null 
;

