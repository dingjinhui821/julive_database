------------------------------------------------------------------------------------
-- ETL -----------------------------------------------------------------------------
------------------------------------------------------------------------------------
set etl_date = concat_ws('-',substr(${hiveconf:etlDate},1,4),substr(${hiveconf:etlDate},5,2),substr(${hiveconf:etlDate},7,2));
--set etl_date = '2020-05-08'; -- yyyy-MM-dd 

set spark.app.name=fact_psft_emp_city_satisfaction_indi; 
set hive.execution.engine=spark;
set spark.yarn.queue=etl;
set spark.default.parallelism=1400;
set spark.executor.cores=2;
set spark.executor.memory=4g;
set spark.executor.instances=8;
set spark.yarn.executor.memoryOverhead=2048;
set hive.prewarm.enabled=true;
set hive.prewarm.numcontainers=14;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000; 

with tmp_consultant_info as ( -- ETL当日在职咨询师信息

select 

emp_id,
emp_name,
adjust_city_id,
adjust_city_name,
direct_leader_id,
direct_leader_name,
indirect_leader_id,
indirect_leader_name,
dept_id,
dept_name,
post_id,
post_name 

from julive_dim.dim_consultant_info 
where pdate = regexp_replace(${hiveconf:etl_date},'-','') 
and job_status = 1 

),
tmp_city as ( -- 城市维度数据 

select 
t1.region,
t1.city_id,
t1.city_name,
t2.city_id        as mgr_city_id,
t1.mgr_city_name  as mgr_city_name,
t2.city_seq       as mgr_city_seq 

from (
select  
t.region,
t.city_id,
t.city_name,
if(t.deputy_city = '主城' or t.deputy_city = '' or t.deputy_city is null,t.city_name,t.mgr_city) as mgr_city_name 

from julive_dim.dim_city t 
) t1 left join julive_dim.dim_city t2 on t1.mgr_city_name = t2.city_name 

),
tmp_adjust_distribute as ( -- 核算上户量 OK 

select 

t1.employee_id                             as emp_id,
count(distinct t1.order_id)                as adjust_distribute_num -- 咨询师上户核算量 

from ods.adjust_incostomer_employee_detail t1 
join julive_dim.dim_clue_info t2 on t1.order_id = t2.clue_id 

where t2.is_distribute = 1 
and to_date(from_unixtime(t1.happen_updatetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.happen_updatetime)) <= date_add(${hiveconf:etl_date},-0) 

group by 
t1.employee_id 

),
tmp_first_distribute as ( -- 首次分配上户量 OK 
select 

tmp.employee_id_new                          as emp_id,
count(distinct tmp.order_id)                 as first_distribute_num -- 咨询师首次分配上户量 

from (
select 

t1.*,
row_number()over(partition by t1.order_id order by t1.confirm_datetime asc) as rn 

from ods.yw_order_confirm_record t1 
where t1.is_confirm = 1 
) tmp 
where tmp.rn = 1 
and to_date(from_unixtime(tmp.confirm_datetime)) >= date_add(${hiveconf:etl_date},-90)
and to_date(from_unixtime(tmp.confirm_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by 
tmp.employee_id_new
),
tmp_called_first_report as ( -- 首电报告 OK 扩展--> 发送首电报告+北斗资料包数量,首电报告+北斗资料包被浏览数量 
-- 原 ： 
-- select 
-- 
-- t1.employee_id                               as emp_id,
-- count(distinct t1.id)                        as first_called_report_num, -- 发送首电报告数量
-- count(distinct if(t1.views >= 1,id,null))    as first_called_report_viewed_num -- 首电报告被浏览数量 
-- 
-- from ods.yw_material_report_template t1 
-- where t1.follow_type = 1 -- 联系 
-- and t1.status = 2 -- 已发送 
-- and to_date(from_unixtime(t1.first_push_datetime)) >= date_add(${hiveconf:etl_date},-90) 
-- and to_date(from_unixtime(t1.first_push_datetime)) <= date_add(${hiveconf:etl_date},-0) 
-- 
-- group by 
-- t1.employee_id 
-- 

select 

tmp.emp_id                    as emp_id,
count(distinct tmp.id)        as first_called_report_num, -- 发送首电报告数量 
count(distinct tmp.viewed_id) as first_called_report_viewed_num -- 首电报告被浏览数量 

from ( 
select 

-- t1.employee_id                               as emp_id,
-- count(distinct t1.id)                        as first_called_report_num, -- 发送首电报告数量
-- count(distinct if(t1.views >= 1,id,null))    as first_called_report_viewed_num -- 首电报告被浏览数量 

t1.employee_id                               as emp_id,
t1.id                                        as id,
if(t1.views >= 1,id,null)                    as viewed_id 

from ods.yw_material_report_template t1 
where t1.follow_type = 1 -- 联系 
and t1.status = 2 -- 已发送 
and to_date(from_unixtime(t1.first_push_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.first_push_datetime)) <= date_add(${hiveconf:etl_date},-0) 

union all 

select 

t1.adviser_id                            as emp_id,
t1.order_id                              as id,
if(t1.event_type = 6,t1.order_id,null)   as viewed_id 

from ods.yw_weixin_track t1 
where t1.share_type in (1,2,3) 
and to_date(from_unixtime(cast(t1.time/1000 as bigint))) >= date_add(${hiveconf:etl_date},-90)
and to_date(from_unixtime(cast(t1.time/1000 as bigint))) <= date_add(${hiveconf:etl_date},-0) 
) tmp 
group by tmp.emp_id 

),
tmp_see_first_report as ( -- 带看首电报告 OK 
select 

t1.employee_id                               as emp_id,
count(distinct t1.id)                        as first_see_report_num, -- 发送首电报告数量
count(distinct if(t1.views >= 1,id,null))    as first_see_report_viewed_num -- 首电报告被浏览数量 

from ods.yw_material_report_template t1 
where t1.follow_type = 2 -- 带看  
and t1.status = 2 -- 已发送 
and to_date(from_unixtime(t1.first_push_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.first_push_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by 
t1.employee_id
),
tmp_first_called_reword as ( -- 首电报告被打赏 OK 扩展 首电报告+北斗资料包打赏数量。 
-- 原 ： 
-- select 
-- 
-- t1.employee_id                             as emp_id,
-- count(distinct t1.order_id)                as first_called_reword_num -- 首电报告被打赏数量 
-- 
-- from ods.yw_employee_reward t1 
-- where t1.status = 3 
-- and t1.business_type = 'FirstReport' 
-- and to_date(from_unixtime(t1.asyn_datetime)) >= date_add(${hiveconf:etl_date},-90) 
-- and to_date(from_unixtime(t1.asyn_datetime)) <= date_add(${hiveconf:etl_date},-0) 
-- 
-- group by 
-- t1.employee_id
-- 

select 

tmp.emp_id                             as emp_id,
count(distinct tmp.order_id)           as first_called_reword_num -- 首电报告被打赏数量 

from ( 
select 

t1.employee_id   as emp_id,
t1.order_id      as order_id 

from ods.yw_employee_reward t1 
where t1.status = 3 
and t1.business_type = 'FirstReport' 
and to_date(from_unixtime(t1.asyn_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.asyn_datetime)) <= date_add(${hiveconf:etl_date},-0) 

union all 

select  

t1.adviser_id as emp_id,
t1.order_id   as order_id --北斗资料包被打赏 

from ods.yw_weixin_track t1 
where t1.share_type in (1, 2, 3)
and t1.event_type = 99
and to_date(from_unixtime(cast(t1.time/1000 as bigint))) >= date_add(${hiveconf:etl_date},-90)
and to_date(from_unixtime(cast(t1.time/1000 as bigint))) <= date_add(${hiveconf:etl_date},-0) 
) tmp 
group by 
tmp.emp_id 

),
tmp_see_reword as ( -- 带看报告被打赏 OK 
select 

t1.employee_id                             as emp_id,
count(distinct t1.order_id)                as see_reword_num -- 带看被打赏数量 

from ods.yw_employee_reward t1 
where t1.status = 3 
and t1.business_type = 'SeeReport' 
and to_date(from_unixtime(t1.asyn_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.asyn_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by 
t1.employee_id
),
tmp_see_project as ( -- 带看量 OK 
select 

t1.see_emp_id                       as emp_id,
sum(see_num)                        as see_num -- 带看量 

from julive_fact.fact_see_project_dtl t1 
where to_date(t1.see_create_time) >= date_add(${hiveconf:etl_date},-90) 
and to_date(t1.see_create_time) <= date_add(${hiveconf:etl_date},-0) 

group by 
t1.see_emp_id
),
tmp_sign as ( -- 签约量 OK 
select 

t1.emp_id                          as emp_id,
sum(sign_contains_cancel_ext_num)  as sign_contains_cancel_ext_num -- 签约量-含退-含外联 

from julive_fact.fact_sign_dtl t1 
where to_date(t1.sign_time) >= date_add(${hiveconf:etl_date},-90)
and to_date(t1.sign_time) <= date_add(${hiveconf:etl_date},-0)

group by 
t1.emp_id
),
tmp_cust_follow as ( -- 新上户跟进频率低数量\日常维护频率太低数量 OK -- 确定时间字段 ？？？
select 

t1.employee_id                                                          as emp_id,
count(distinct if(t1.label_name='新上户跟进频率低',t1.order_id,null)) as new_cust_follow_low_num, -- 新上户跟进频率低数量 20200618注：新上户跟进频率太低 -> 新上户跟进频率低
count(distinct if(t1.label_name='日常维护频率太低',t1.order_id,null))   as day_follow_low_num -- 日常维护频率太低数量 

from ods.yw_follow_business_tag t1 
where (t1.label_name='新上户跟进频率低' or t1.label_name='日常维护频率太低')
and to_date(from_unixtime(t1.business_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.business_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by 
t1.employee_id
),
tmp_clue_close_nonsee as ( -- 未带看关闭数量 OK 
select 

t1.emp_id                       as emp_id,
count(distinct t1.clue_id)      as clue_close_nonsee_num -- 未带看关闭线索数量 

from julive_dim.dim_clue_info t1 
where t1.intent = 1 -- 无意向线索 
and t1.clue_id not in ( -- 没有带看 
select clue_id 
from julive_fact.fact_see_project_dtl 
where status >= 40 
  and status < 60 
group by clue_id 
) 
and t1.create_date >= date_add(${hiveconf:etl_date},-90) 
and t1.create_date <= date_add(${hiveconf:etl_date},-0) 

group by 
t1.emp_id
),
tmp_return_visit as ( -- 无意向评价 OK 
select 

t1.emp_id                                                                               as emp_id,
-- 评价线索量 
count(distinct if(t1.final_grade = 0,t1.clue_id,null))                                  as rv_final_grade_0_num,
count(distinct if(t1.final_grade = 1,t1.clue_id,null))                                  as rv_final_grade_1_num,
count(distinct if(t1.final_grade = 2,t1.clue_id,null))                                  as rv_final_grade_2_num,
count(distinct if(t1.final_grade = 3,t1.clue_id,null))                                  as rv_final_grade_3_num,
count(distinct if(t1.final_grade = 4,t1.clue_id,null))                                  as rv_final_grade_4_num,
count(distinct if(t1.final_grade = 5,t1.clue_id,null))                                  as rv_final_grade_5_num,
count(distinct if(t1.is_txt_comment = 1,t1.clue_id,null))                               as rv_hastxt_comment_num,
count(distinct if(t1.txt_comment_num < 15,t1.clue_id,null))                             as rv_txt_comment_lt15_num,
count(distinct if(t1.txt_comment_num >= 15,t1.clue_id,null))                            as rv_txt_comment_ge15_num,
count(distinct if(t1.continue_server = 0,t1.clue_id,null))                              as continue_server_0_num,
count(distinct if(t1.continue_server = 1,t1.clue_id,null))                              as continue_server_1_num,
count(distinct if(t1.continue_server = 2,t1.clue_id,null))                              as continue_server_2_num,
count(distinct if(t1.txt_comment_num < 15 and t1.continue_server = 0,t1.clue_id,null))  as txt_comment_lt15_cs0_num,
count(distinct if(t1.txt_comment_num < 15 and t1.continue_server = 1,t1.clue_id,null))  as txt_comment_lt15_cs1_num,
count(distinct if(t1.txt_comment_num < 15 and t1.continue_server = 2,t1.clue_id,null))  as txt_comment_lt15_cs2_num,
count(distinct if(t1.txt_comment_num >= 15 and t1.continue_server = 0,t1.clue_id,null)) as txt_comment_ge15_cs0_num,
count(distinct if(t1.txt_comment_num >= 15 and t1.continue_server = 1,t1.clue_id,null)) as txt_comment_ge15_cs1_num,
count(distinct if(t1.txt_comment_num >= 15 and t1.continue_server = 2,t1.clue_id,null)) as txt_comment_ge15_cs2_num

from julive_fact.fact_return_visit t1 
join (

select t1.clue_id 
from julive_dim.dim_clue_info t1 
where t1.intent = 1 -- 无意向线索 
and t1.clue_id not in ( -- 没有带看 
select clue_id 
from julive_fact.fact_see_project_dtl 
where status >= 40 and status < 60 
group by clue_id 
) 
and to_date(t1.intent_low_time) >= date_add(${hiveconf:etl_date},-90) 
and to_date(t1.intent_low_time) <= date_add(${hiveconf:etl_date},-0) 
group by t1.clue_id 
) t2 on t1.clue_id = t2.clue_id 
where t1.is_init_eval = 1 -- 客户主动评价 
and to_date(t1.visit_time) >= date_add(${hiveconf:etl_date},-90) 
and to_date(t1.visit_time) <= date_add(${hiveconf:etl_date},-0) 

group by 
t1.emp_id 
),
tmp_see_comment as ( -- 带看评价 OK 
select 

t1.see_emp_id                                                                           as emp_id,
-- 评价线索量 
count(distinct if(t1.final_grade = 0,t1.clue_id,null))                                  as sc_final_grade_0_num,
count(distinct if(t1.final_grade = 1,t1.clue_id,null))                                  as sc_final_grade_1_num,
count(distinct if(t1.final_grade = 2,t1.clue_id,null))                                  as sc_final_grade_2_num,
count(distinct if(t1.final_grade = 3,t1.clue_id,null))                                  as sc_final_grade_3_num,
count(distinct if(t1.final_grade = 4,t1.clue_id,null))                                  as sc_final_grade_4_num,
count(distinct if(t1.final_grade = 5,t1.clue_id,null))                                  as sc_final_grade_5_num,
count(distinct if(t1.is_txt_comment = 1,t1.clue_id,null))                               as sc_hastxt_comment_num,
count(distinct if(t1.txt_comment_num < 30,t1.clue_id,null))                             as sc_txt_comment_lt30_num,
count(distinct if(t1.txt_comment_num >= 30,t1.clue_id,null))                            as sc_txt_comment_ge30_num,
count(distinct if(t1.probability < 90,t1.clue_id,null))                                 as nps_lt90_num,
count(distinct if(t1.probability >= 90,t1.clue_id,null))                                as nps_ge90_num,
count(distinct if(t1.txt_comment_num < 30 and t1.probability < 90,t1.clue_id,null))     as txt_comment_lt30_nps_lt90_num,
count(distinct if(t1.txt_comment_num < 30 and t1.probability >= 90,t1.clue_id,null))    as txt_comment_lt30_nps_gt90_num,
count(distinct if(t1.txt_comment_num >= 30 and t1.probability < 90,t1.clue_id,null))    as txt_comment_ge30_nps_lt90_num,
count(distinct if(t1.txt_comment_num >= 30 and t1.probability >= 90,t1.clue_id,null))   as txt_comment_ge30_nps_gt90_num

from julive_fact.fact_see_comment t1 
where to_date(t1.visit_time) >= date_add(${hiveconf:etl_date},-90) 
and to_date(t1.visit_time) <= date_add(${hiveconf:etl_date},-0) 

group by 
t1.see_emp_id 
),
tmp_sign_comment as ( -- 签约评论-订单评论 OK 
select 
tmp1.emp_id                         as emp_id,
sum(tmp1.sign_comment_num)          as sign_comment_num,
sum(tmp1.sign_comment_good_num)     as sign_comment_good_num,
sum(tmp1.sign_comment_nongood_num)  as sign_comment_nongood_num

from (

select 

t1.employee_id                                                                         as emp_id,
count(distinct if(length(t1.comment_user) > 0,t1.order_id,null))                       as sign_comment_num,
count(distinct if(t1.marvellous = 1,t1.order_id,null))                                 as sign_comment_good_num,
count(distinct if(length(t1.comment_user) > 0 and t1.marvellous = 0,t1.order_id,null)) as sign_comment_nongood_num 

from ods.yw_order_comment t1 
where to_date(from_unixtime(t1.create_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.create_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by 
t1.employee_id 

union all

select 
t1.employee_id                                                                         as emp_id,
count(distinct if(length(t1.global_comment) > 0
      and t2.order_id is null
      and t1.status = 1 
      and t1.global_comment!='null',t1.order_id,null))              as sign_comment_num,--评论次数
count(distinct if(t1.status=1
      and t2.order_id is null 
      and length(t1.global_comment) > 100 ,t1.order_id,null))       as sign_comment_good_num,--评论加精
count(distinct if(length(t1.global_comment) > 0
      and t2.order_id is null 
      and length(t1.global_comment) <=100 
      and t1.global_comment !='null' 
      and t1.status = 1,t1.order_id,null))                          as sign_comment_nongood_num--评论未加精 

from ods.yw_sign_comment t1 
left join ods.yw_order_comment t2 on t1.order_id = t2.order_id

where to_date(from_unixtime(t1.create_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.create_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by 
t1.employee_id )tmp1
group by tmp1.emp_id
),
tmp_cust_recommend as ( -- 友介客户数量 OK 
select 

t1.emp_id                                          as emp_id,
count(distinct if(t1.source = 17,t1.clue_id,null)) as cust_recommend_num 

from julive_dim.dim_clue_info t1 
where t1.create_date >= date_add(${hiveconf:etl_date},-90) 
and t1.create_date <= date_add(${hiveconf:etl_date},-0) 

group by 
t1.emp_id 
),
tmp_customer_complaints as ( -- 客户投诉量 
select   

a.employee_id                 as emp_id, 
count(a.id)                   as cust_comp_num 

from ods.yw_customer_complaints a 
where a.problem_cat_level_one = 1
and a.complaint_status = 0
and to_date(from_unixtime(a.complaints_occur_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(a.complaints_occur_datetime)) <= date_add(${hiveconf:etl_date},-0) 
group by
a.employee_id
) 

insert overwrite table julive_fact.fact_psft_emp_city_satisfaction_indi partition(pdate) 

select 

emp.emp_id,
emp.emp_name,
t16.region                                         as region,
coalesce(t16.mgr_city_id,emp.adjust_city_id)       as adjust_city_id,
coalesce(t16.mgr_city_name,emp.adjust_city_name)   as adjust_city_name,
t16.mgr_city_seq                                   as adjust_city_seq,

emp.direct_leader_id,
emp.direct_leader_name,
emp.indirect_leader_id,
emp.indirect_leader_name,
emp.dept_id,
emp.dept_name,
emp.post_id,
emp.post_name,

-- 指标 
coalesce(t1.adjust_distribute_num,0)            as adjust_distribute_num, -- 咨询师上户核算量
coalesce(t2.first_distribute_num,0)             as first_distribute_num, -- 咨询师首次分配上户量
coalesce(t3.first_called_report_num,0)          as first_called_report_num, -- 发送首电报告数量
coalesce(t3.first_called_report_viewed_num,0)   as first_called_report_viewed_num , -- 首电报告被浏览数量 
coalesce(t4.first_see_report_num,0)             as see_report_num, -- 发送首电报告数量
coalesce(t4.first_see_report_viewed_num,0)      as see_report_viewed_num, -- 首电报告被浏览数量
coalesce(t5.first_called_reword_num,0)          as first_called_reword_num, -- 首电报告被打赏数量 
coalesce(t6.see_reword_num,0)                   as see_reword_num, -- 带看被打赏数量
coalesce(t7.see_num,0)                          as see_num, -- 带看量
coalesce(t8.sign_contains_cancel_ext_num,0)     as sign_contains_cancel_ext_num, -- 签约量-含退-含外联
coalesce(t9.new_cust_follow_low_num,0)          as new_cust_follow_low_num, -- 新上户跟进频率低数量
coalesce(t9.day_follow_low_num,0)               as day_follow_low_num, -- 日常维护频率太低数量 
coalesce(t10.clue_close_nonsee_num,0)           as clue_close_nonsee_num, -- 未带看关闭线索数量
coalesce(t11.rv_final_grade_0_num,0)            as rv_final_grade_0_num,
coalesce(t11.rv_final_grade_1_num,0)            as rv_final_grade_1_num,
coalesce(t11.rv_final_grade_2_num,0)            as rv_final_grade_2_num,
coalesce(t11.rv_final_grade_3_num,0)            as rv_final_grade_3_num,
coalesce(t11.rv_final_grade_4_num,0)            as rv_final_grade_4_num,
coalesce(t11.rv_final_grade_5_num,0)            as rv_final_grade_5_num,
coalesce(t11.rv_hastxt_comment_num,0)           as rv_hastxt_comment_num,
coalesce(t11.rv_txt_comment_lt15_num,0)         as rv_txt_comment_lt15_num,
coalesce(t11.rv_txt_comment_ge15_num,0)         as rv_txt_comment_ge15_num ,
coalesce(t11.continue_server_0_num,0)           as continue_server_0_num,
coalesce(t11.continue_server_1_num,0)           as continue_server_1_num,
coalesce(t11.continue_server_2_num,0)           as continue_server_2_num,
coalesce(t11.txt_comment_lt15_cs0_num,0)        as txt_comment_lt15_cs0_num,
coalesce(t11.txt_comment_lt15_cs1_num,0)        as txt_comment_lt15_cs1_num,
coalesce(t11.txt_comment_lt15_cs2_num,0)        as txt_comment_lt15_cs2_num,
coalesce(t11.txt_comment_ge15_cs0_num,0)        as txt_comment_ge15_cs0_num,
coalesce(t11.txt_comment_ge15_cs1_num,0)        as txt_comment_ge15_cs1_num,
coalesce(t11.txt_comment_ge15_cs2_num,0)        as txt_comment_ge15_cs2_num,
coalesce(t12.sc_final_grade_0_num,0)            as sc_final_grade_0_num,
coalesce(t12.sc_final_grade_1_num,0)            as sc_final_grade_1_num,
coalesce(t12.sc_final_grade_2_num,0)            as sc_final_grade_2_num,
coalesce(t12.sc_final_grade_3_num,0)            as sc_final_grade_3_num,
coalesce(t12.sc_final_grade_4_num,0)            as sc_final_grade_4_num,
coalesce(t12.sc_final_grade_5_num,0)            as sc_final_grade_5_num,
coalesce(t12.sc_hastxt_comment_num,0)           as sc_hastxt_comment_num,
coalesce(t12.sc_txt_comment_lt30_num,0)         as sc_txt_comment_lt30_num,
coalesce(t12.sc_txt_comment_ge30_num,0)         as sc_txt_comment_ge30_num,
coalesce(t12.nps_lt90_num,0)                    as nps_lt90_num,
coalesce(t12.nps_ge90_num,0)                    as nps_ge90_num,
coalesce(t12.txt_comment_lt30_nps_lt90_num,0)   as txt_comment_lt30_nps_lt90_num,
coalesce(t12.txt_comment_lt30_nps_gt90_num,0)   as txt_comment_lt30_nps_gt90_num,
coalesce(t12.txt_comment_ge30_nps_lt90_num,0)   as txt_comment_ge30_nps_lt90_num,
coalesce(t12.txt_comment_ge30_nps_gt90_num,0)   as txt_comment_ge30_nps_gt90_num,
coalesce(t13.sign_comment_num,0)                as sign_comment_num,
coalesce(t13.sign_comment_good_num,0)           as sign_comment_good_num,
coalesce(t13.sign_comment_nongood_num,0)        as sign_comment_nongood_num,
coalesce(t14.cust_recommend_num,0)              as cust_recommend_num,
coalesce(t15.cust_comp_num,0)                   as cust_comp_num,
0                                               as first_called_report_shared_num,
0                                               as see_reword_shared_num,
current_timestamp()                             as etl_time,
regexp_replace(${hiveconf:etl_date},'-','')     as pdate 

from tmp_consultant_info emp 
left join tmp_adjust_distribute   t1  on emp.emp_id = t1.emp_id 
left join tmp_first_distribute    t2  on emp.emp_id = t2.emp_id 
left join tmp_called_first_report t3  on emp.emp_id = t3.emp_id 
left join tmp_see_first_report    t4  on emp.emp_id = t4.emp_id 
left join tmp_first_called_reword t5  on emp.emp_id = t5.emp_id 
left join tmp_see_reword          t6  on emp.emp_id = t6.emp_id 
left join tmp_see_project         t7  on emp.emp_id = t7.emp_id 
left join tmp_sign                t8  on emp.emp_id = t8.emp_id 
left join tmp_cust_follow         t9  on emp.emp_id = t9.emp_id 
left join tmp_clue_close_nonsee   t10 on emp.emp_id = t10.emp_id 
left join tmp_return_visit        t11 on emp.emp_id = t11.emp_id 
left join tmp_see_comment         t12 on emp.emp_id = t12.emp_id 
left join tmp_sign_comment        t13 on emp.emp_id = t13.emp_id 
left join tmp_cust_recommend      t14 on emp.emp_id = t14.emp_id 
left join tmp_customer_complaints t15 on emp.emp_id = t15.emp_id 

left join tmp_city                t16 on emp.adjust_city_id = t16.city_id 
;


