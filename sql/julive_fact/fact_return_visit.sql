-------------------------------------------------------------------------------
-- ETL ------------------------------------------------------------------------
-------------------------------------------------------------------------------
set hive.execution.engine=spark;
set spark.app.name=fact_return_visit;
set mapred.job.name=fact_return_visit;
set spark.yarn.queue=etl;
insert overwrite table julive_fact.fact_return_visit 

select 

t1.order_id                                                                                                  as clue_id,
t1.employee_id                                                                                               as emp_id,
t1.employee_name                                                                                             as emp_name,
t1.employee_leader_id                                                                                        as emp_leader_id,
t1.employee_leader_realname                                                                                  as emp_leader_name,

t1.city_id                                                                                                   as city_id,
t4.city_name                                                                                                 as city_name,
t4.city_seq                                                                                                  as city_seq,

t3.customer_intent_city_id                                                                                   as customer_intent_city_id,-- 20191015 
t3.customer_intent_city_name                                                                                 as customer_intent_city_name,-- 20191015 
t3.customer_intent_city_seq                                                                                  as customer_intent_city_seq,-- 20191015 

if(t1.creator is null or t1.creator = '',1,0)                                                                as is_init_eval, -- 20191028添加 
t1.continue_server                                                                                           as continue_server, -- 20191028添加 
if(t1.employee_grade >= coalesce(t2.final_grade,0),t1.employee_grade,t2.final_grade)                         as final_grade,
if(t1.note is not null and t1.note != '',1,0)                                                                as is_txt_comment,
length(t1.note)                                                                                              as txt_comment_num, -- 20191028添加 
(1 + coalesce(t2.comment_num,0))                                                                             as comment_num,

if(t1.employee_grade is null,from_unixtime(t1.visit_datetime),if(t1.employee_grade >= coalesce(t2.final_grade,0),from_unixtime(t1.visit_datetime),t2.business_happen_time)) as visit_time,---2020-01-19修改
coalesce(from_unixtime(t1.create_datetime),t2.task_grade_time)                                               as create_time,
current_timestamp()                                                                                          as etl_time 

from ods.yw_return_visit t1 
left join ( 

select 

t.rv_id,
t.final_grade,
t.business_happen_time,
t.task_grade_time,
t.comment_num 

from (
select 

business_id                                                          as rv_id,
final_grade                                                          as final_grade,
from_unixtime(business_happen_datetime)                              as business_happen_time, -- 业务发生时间
from_unixtime(task_grade_datetime)                                   as task_grade_time, -- 追回评价时间
count(1)over(partition by business_id)                               as comment_num,
row_number()over(partition by business_id order by final_grade desc) as rn 

from ods.yw_replevy 
where type = 2 -- 无意向追回 
and final_grade is not null 
) t 
where t.rn = 1 

) t2 on t1.id = t2.rv_id 

left join julive_dim.dim_clue_info t3 on t1.order_id = t3.clue_id 
left join julive_dim.dim_city t4 on t1.city_id = t4.city_id 

---2020-01-16注释 where t1.status = 1 -- 已回访 
;

