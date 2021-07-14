
------------------------------------------------------------------------------------
-- ETL -----------------------------------------------------------------------------
------------------------------------------------------------------------------------
set etl_date = concat_ws('-',substr(${hiveconf:etlDate},1,4),substr(${hiveconf:etlDate},5,2),substr(${hiveconf:etlDate},7,2));
-- set etl_date = '2020-05-08';
set spark.app.name=fact_psft_city_satisfaction_indi; 
set hive.execution.engine=spark;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=8;
set spark.yarn.executor.memoryOverhead=2048; 
set hive.exec.dynamic.partition.mode=nonstrict;

-- 修改记录 ： 副城数据上卷到主城 

with tmp_city as ( -- 城市维度数据 

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

coalesce(t3.mgr_city_id, t2.customer_intent_city_id) as city_id,
max(t3.mgr_city_name)                                as city_name,
max(t3.mgr_city_seq)                                 as city_seq,

count(distinct t1.order_id)                          as adjust_distribute_num 

from ods.adjust_incostomer_employee_detail t1 
join julive_dim.dim_clue_info t2 on t1.order_id = t2.clue_id
left join tmp_city t3 on t2.customer_intent_city_id = t3.city_id -- 20200618添加 

where t2.is_distribute = 1 
and to_date(from_unixtime(t1.happen_updatetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.happen_updatetime)) <= date_add(${hiveconf:etl_date},-0) 
   
group by coalesce(t3.mgr_city_id, t2.customer_intent_city_id)
 
),
tmp_first_distribute as ( -- 首次分配上户量 OK 

select 

tmp.mgr_city_id                              as city_id,
count(distinct tmp.order_id)                 as first_distribute_num -- 首次分配上户量 

from (

select 

t1.*,
coalesce(t3.mgr_city_id, t2.customer_intent_city_id) as mgr_city_id,
t3.mgr_city_name,
row_number()over(partition by t1.order_id order by t1.confirm_datetime asc) as rn 

from ods.yw_order_confirm_record t1 
join julive_dim.dim_clue_info t2 on t1.order_id = t2.clue_id 
left join tmp_city t3 on t2.customer_intent_city_id = t3.city_id -- 20200618添加 

where t1.is_confirm = 1 

) tmp 

where tmp.rn = 1 
and to_date(from_unixtime(tmp.confirm_datetime)) >= date_add(${hiveconf:etl_date},-90)
and to_date(from_unixtime(tmp.confirm_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by tmp.mgr_city_id
  
),
tmp_called_first_report as ( -- 首电报告 OK 

select 

tmp.city_id                                   as city_id,
count(distinct tmp.id)                        as first_called_report_num, -- 发送首电报告数量
count(distinct tmp.viewed_id)                 as first_called_report_viewed_num -- 首电报告被浏览数量 

from ( 

select 

coalesce(t3.mgr_city_id, t2.customer_intent_city_id)    as city_id,
t1.id                                                   as id,
if(t1.views >= 1,id,null)                               as viewed_id 

from ods.yw_material_report_template t1 
left join julive_dim.dim_clue_info t2 on t1.order_id = t2.clue_id 
left join tmp_city t3 on t2.customer_intent_city_id = t3.city_id -- 20200618添加 

where t1.follow_type = 1 -- 联系 
and t1.status = 2 -- 已发送 
and to_date(from_unixtime(t1.first_push_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.first_push_datetime)) <= date_add(${hiveconf:etl_date},-0) 

union all 

select 

coalesce(t3.mgr_city_id, t2.customer_intent_city_id)               as city_id,
t1.order_id                                                        as id,
if(t1.event_type = 6,t1.order_id,null)                             as viewed_id 

from ods.yw_weixin_track t1 
join julive_dim.dim_clue_info t2 on t1.order_id = t2.clue_id 
left join tmp_city t3 on t2.customer_intent_city_id = t3.city_id -- 20200618添加 

where t1.share_type in (1,2,3) 
and to_date(from_unixtime(cast(t1.time/1000 as bigint))) >= date_add(${hiveconf:etl_date},-90)
and to_date(from_unixtime(cast(t1.time/1000 as bigint))) <= date_add(${hiveconf:etl_date},-0) 

) tmp 
group by tmp.city_id 

),
tmp_see_first_report as ( -- 带看首电报告 OK 

select 

coalesce(t3.mgr_city_id, t2.customer_intent_city_id)                as city_id,
count(distinct t1.id)                                               as first_see_report_num, -- 发送首电报告数量
count(distinct if(t1.views >= 1,id,null))                           as first_see_report_viewed_num -- 首电报告被浏览数量 

from ods.yw_material_report_template t1 
left join julive_dim.dim_clue_info t2 on t1.order_id = t2.clue_id 
left join tmp_city t3 on t2.customer_intent_city_id = t3.city_id -- 20200618添加 

where t1.follow_type = 2 -- 带看  
and t1.status = 2 -- 已发送 
and to_date(from_unixtime(t1.first_push_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.first_push_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by coalesce(t3.mgr_city_id, t2.customer_intent_city_id)

),
tmp_first_called_reword as ( -- 首电报告被打赏 OK 

select 

tmp.city_id                             as city_id,
count(distinct tmp.order_id)            as first_called_reword_num -- 首电报告被打赏数量 

from ( 

select 

coalesce(t3.mgr_city_id, t2.customer_intent_city_id)   as city_id,
t1.order_id                                            as order_id 

from ods.yw_employee_reward t1 
join julive_dim.dim_clue_info t2 on t1.order_id = t2.clue_id 
left join tmp_city t3 on t2.customer_intent_city_id = t3.city_id -- 20200618添加 

where t1.status = 3 
and t1.business_type = 'FirstReport' 
and to_date(from_unixtime(t1.asyn_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.asyn_datetime)) <= date_add(${hiveconf:etl_date},-0) 

union all 

select  

coalesce(t3.mgr_city_id, t2.customer_intent_city_id) as city_id,
t1.order_id                                          as order_id --北斗资料包被打赏 

from ods.yw_weixin_track t1 
join julive_dim.dim_clue_info t2 on t1.order_id = t2.clue_id 
left join tmp_city t3 on t2.customer_intent_city_id = t3.city_id -- 20200618添加 

where t1.share_type in (1, 2, 3)
and t1.event_type = 99
and to_date(from_unixtime(cast(t1.time/1000 as bigint))) >= date_add(${hiveconf:etl_date},-90)
and to_date(from_unixtime(cast(t1.time/1000 as bigint))) <= date_add(${hiveconf:etl_date},-0) 

) tmp 
group by tmp.city_id  

),
tmp_see_reword as ( -- 带看报告被打赏 OK 

select 

coalesce(t4.mgr_city_id, t3.customer_intent_city_id)           as city_id,
count(distinct t1.order_id)                                    as see_reword_num -- 带看被打赏数量 

from ods.yw_employee_reward t1 
-- join ods.yw_material_report_template t2 
-- on t1.employee_id = t2.employee_id 
-- and to_date(from_unixtime(t1.asyn_datetime)) = to_date(from_unixtime(t2.first_push_datetime)) 
-- and t1.order_id = t2.order_id 
left join julive_dim.dim_clue_info t3 on t1.order_id = t3.clue_id 
left join tmp_city t4 on t3.customer_intent_city_id = t4.city_id -- 20200618添加 

where t1.status = 3 
-- and t2.follow_type = 2
and t1.business_type = 'SeeReport' 
and to_date(from_unixtime(t1.asyn_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.asyn_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by coalesce(t4.mgr_city_id, t3.customer_intent_city_id)

),
tmp_see_project as ( -- 带看量 OK 

select 

coalesce(t2.mgr_city_id,t1.project_city_id)          as city_id,
sum(see_num)                                         as see_num -- 带看量 

from julive_fact.fact_see_project_dtl t1 
left join tmp_city t2 on t1.project_city_id = t2.city_id -- 20200618添加 

where to_date(t1.see_create_time) >= date_add(${hiveconf:etl_date},-90) 
and to_date(t1.see_create_time) <= date_add(${hiveconf:etl_date},-0) 

group by coalesce(t2.mgr_city_id,t1.project_city_id)

),
tmp_sign as ( -- 签约量 OK 

select 

coalesce(t2.mgr_city_id,t1.project_city_id)           as city_id,
sum(sign_contains_cancel_ext_num)                     as sign_contains_cancel_ext_num -- 签约量-含退-含外联 

from julive_fact.fact_sign_dtl t1 
left join tmp_city t2 on t1.project_city_id = t2.city_id -- 20200618添加 

where to_date(t1.sign_time) >= date_add(${hiveconf:etl_date},-90)
and to_date(t1.sign_time) <= date_add(${hiveconf:etl_date},-0)

group by coalesce(t2.mgr_city_id,t1.project_city_id)

),
tmp_cust_follow as ( -- 新上户跟进频率低数量\日常维护频率太低数量 OK -- 确定时间字段 ？？？

select 

coalesce(t3.mgr_city_id,t2.customer_intent_city_id)                     as city_id,
count(distinct if(t1.label_name='新上户跟进频率低',t1.order_id,null)) as new_cust_follow_low_num, -- 新上户跟进频率低数量 20200618注：新上户跟进频率太低 -> 新上户跟进频率低
count(distinct if(t1.label_name='日常维护频率太低',t1.order_id,null))   as day_follow_low_num -- 日常维护频率太低数量 

from ods.yw_follow_business_tag t1 
join julive_dim.dim_clue_info t2 on t1.order_id = t2.clue_id 
left join tmp_city t3 on t2.customer_intent_city_id = t3.city_id 

where (t1.label_name='新上户跟进频率低' or t1.label_name='日常维护频率太低')
and to_date(from_unixtime(t1.business_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.business_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by coalesce(t3.mgr_city_id,t2.customer_intent_city_id) 

),
tmp_clue_close_nonsee as ( -- 未带看关闭数量 OK 

select 

coalesce(t2.mgr_city_id,t1.customer_intent_city_id)    as city_id,
count(distinct t1.clue_id)                             as clue_close_nonsee_num -- 未带看关闭线索数量 

from julive_dim.dim_clue_info t1 
left join tmp_city t2 on t1.customer_intent_city_id = t2.city_id 

where t1.intent = 1 -- 无意向线索 
and t1.clue_id not in ( -- 没有带看 

select clue_id 
from julive_fact.fact_see_project_dtl 
where status >= 40 and status < 60 
group by clue_id 

) 
and t1.create_date >= date_add(${hiveconf:etl_date},-90) 
and t1.create_date <= date_add(${hiveconf:etl_date},-0) 

group by coalesce(t2.mgr_city_id,t1.customer_intent_city_id) 

),
tmp_return_visit as ( -- 无意向评价 OK 

select 

coalesce(t3.mgr_city_id,t1.customer_intent_city_id)                                     as city_id,

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
left join tmp_city t3 on t1.customer_intent_city_id = t3.city_id 

where t1.is_init_eval = 1 -- 客户主动评价 
and to_date(t1.visit_time) >= date_add(${hiveconf:etl_date},-90) 
and to_date(t1.visit_time) <= date_add(${hiveconf:etl_date},-0) 

group by coalesce(t3.mgr_city_id,t1.customer_intent_city_id) 

),
tmp_see_comment as ( -- 带看评价 OK 

select 

coalesce(t2.mgr_city_id,t1.project_city_id)                                             as city_id,

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
left join tmp_city t2 on t1.project_city_id = t2.city_id 

where to_date(t1.visit_time) >= date_add(${hiveconf:etl_date},-90) 
and to_date(t1.visit_time) <= date_add(${hiveconf:etl_date},-0) 

group by coalesce(t2.mgr_city_id,t1.project_city_id) 

),
tmp_sign_comment as ( -- 签约评论-订单评论 OK 

select 
tmp1.city_id                        as city_id,
sum(tmp1.sign_comment_num)          as sign_comment_num,
sum(tmp1.sign_comment_good_num)     as sign_comment_good_num,
sum(tmp1.sign_comment_nongood_num)  as sign_comment_nongood_num
from(

select 

coalesce(t3.mgr_city_id,t2.customer_intent_city_id)                                    as city_id,
count(distinct if(length(t1.comment_user) > 0,t1.order_id,null))                       as sign_comment_num,
count(distinct if(t1.marvellous = 1,t1.order_id,null))                                 as sign_comment_good_num,
count(distinct if(length(t1.comment_user) > 0 and t1.marvellous = 0,t1.order_id,null)) as sign_comment_nongood_num 

from ods.yw_order_comment t1 
left join julive_dim.dim_clue_info t2 on t1.order_id = t2.clue_id 
left join tmp_city t3 on t2.customer_intent_city_id = t3.city_id 

where to_date(from_unixtime(t1.create_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.create_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by coalesce(t3.mgr_city_id,t2.customer_intent_city_id) 


union all

select 

coalesce(t3.mgr_city_id,t2.customer_intent_city_id)                 as city_id,
count(distinct if(length(t1.global_comment) > 0
      and t4.order_id is null
      and t1.status = 1 
      and t1.global_comment!='null',t1.order_id,null))              as sign_comment_num,--评论次数
count(distinct if(t1.status=1
      and t4.order_id is null 
      and length(t1.global_comment) > 100 ,t1.order_id,null))       as sign_comment_good_num,--评论加精
count(distinct if(length(t1.global_comment) > 0
      and t4.order_id is null 
      and length(t1.global_comment) <=100 
      and t1.global_comment !='null' 
      and t1.status = 1,t1.order_id,null))                          as sign_comment_nongood_num--评论未加精 

from ods.yw_sign_comment t1 
left join julive_dim.dim_clue_info t2 on t1.order_id = t2.clue_id 
left join tmp_city t3 on t2.customer_intent_city_id = t3.city_id
left join ods.yw_order_comment t4 on t1.order_id = t4.order_id 

where to_date(from_unixtime(t1.create_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.create_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by coalesce(t3.mgr_city_id,t2.customer_intent_city_id))tmp1

group by tmp1.city_id
),
tmp_cust_recommend as ( -- 友介客户数量 OK 

select 

coalesce(t2.mgr_city_id,t1.customer_intent_city_id) as city_id,
count(distinct if(t1.source = 17,t1.clue_id,null))  as cust_recommend_num 

from julive_dim.dim_clue_info t1  
left join tmp_city t2 on t1.customer_intent_city_id = t2.city_id 

where t1.create_date >= date_add(${hiveconf:etl_date},-90) 
and t1.create_date <= date_add(${hiveconf:etl_date},-0) 

group by coalesce(t2.mgr_city_id,t1.customer_intent_city_id) 

),
tmp_customer_complaints as ( -- 客户投诉量 

select   

coalesce(t3.mgr_city_id,t2.customer_intent_city_id)   as city_id,
count(t1.id)                                          as cust_comp_num 

from ods.yw_customer_complaints t1 
left join julive_dim.dim_clue_info t2 on t1.order_id = t2.clue_id 
left join tmp_city t3 on t2.customer_intent_city_id = t3.city_id 

where t1.problem_cat_level_one = 1
and t1.complaint_status = 0
and to_date(from_unixtime(t1.complaints_occur_datetime)) >= date_add(${hiveconf:etl_date},-90) 
and to_date(from_unixtime(t1.complaints_occur_datetime)) <= date_add(${hiveconf:etl_date},-0) 

group by coalesce(t3.mgr_city_id,t2.customer_intent_city_id) 

) 

-- 需求数据
insert overwrite table julive_fact.fact_psft_city_satisfaction_indi partition(pdate)
 
select 
tmp1.region                                                                                as region,
tmp1.city_id                                                                               as city_id,
tmp1.city_name                                                                             as city_name,
tmp1.city_seq                                                                              as city_seq,

if(tmp1.adjust_distribute_num is not null,tmp1.adjust_distribute_num,0)                    as adjust_distribute_num, 
if(tmp1.first_distribute_num is not null,tmp1.first_distribute_num,0)                      as first_distribute_num,
if(tmp1.first_called_report_num is not null,tmp1.first_called_report_num,0)                as first_called_report_num, 
if(tmp1.first_called_report_viewed_num is not null,tmp1.first_called_report_viewed_num,0)  as first_called_report_viewed_num, 
if(tmp1.see_report_num is not null,tmp1.see_report_num, 0)                                 as see_report_num, 
if(tmp1.see_report_viewed_num is not null,tmp1.see_report_viewed_num,0)                    as see_report_viewed_num,
if(tmp1.first_called_reword_num is not null,tmp1.first_called_reword_num,0)                as first_called_reword_num, 
if(tmp1.see_reword_num is not null,tmp1.see_reword_num, 0)                                 as see_reword_num, 
if(tmp1.see_num is not null,tmp1.see_num,0)                                                as see_num, 
if(tmp1.sign_contains_cancel_ext_num is not null,tmp1.sign_contains_cancel_ext_num,0)      as sign_contains_cancel_ext_num, 
if(tmp1.new_cust_follow_low_num is not null,tmp1.new_cust_follow_low_num,0)                as new_cust_follow_low_num, 
if(tmp1.day_follow_low_num  is not null,tmp1.day_follow_low_num,0)                         as day_follow_low_num, 
if(tmp1.clue_close_nonsee_num is not null,tmp1.clue_close_nonsee_num,0)                    as clue_close_nonsee_num, 
if(tmp1.rv_final_grade_0_num is not null,tmp1.rv_final_grade_0_num,0)                      as rv_final_grade_0_num,
if(tmp1.rv_final_grade_1_num is not null,tmp1.rv_final_grade_1_num,0)                      as rv_final_grade_1_num,
if(tmp1.rv_final_grade_2_num is not null,tmp1.rv_final_grade_2_num,0)                      as rv_final_grade_2_num,
if(tmp1.rv_final_grade_3_num is not null,tmp1.rv_final_grade_3_num,0)                      as rv_final_grade_3_num,
if(tmp1.rv_final_grade_4_num is not null,tmp1.rv_final_grade_4_num,0)                      as rv_final_grade_4_num,
if(tmp1.rv_final_grade_5_num is not null,tmp1.rv_final_grade_5_num,0)                      as rv_final_grade_5_num,
if(tmp1.rv_hastxt_comment_num is not null,tmp1.rv_hastxt_comment_num,0)                    as rv_hastxt_comment_num,
if(tmp1.rv_txt_comment_lt15_num is not null,tmp1.rv_txt_comment_lt15_num,0)                as rv_txt_comment_lt15_num,
if(tmp1.rv_txt_comment_ge15_num is not null,tmp1.rv_txt_comment_ge15_num,0)                as rv_txt_comment_ge15_num,
if(tmp1.continue_server_0_num is not null,tmp1.continue_server_0_num,0)                    as continue_server_0_num,
if(tmp1.continue_server_1_num is not null,tmp1.continue_server_1_num,0)                    as continue_server_1_num,
if(tmp1.continue_server_2_num is not null,tmp1.continue_server_2_num,0)                    as continue_server_2_num,
if(tmp1.txt_comment_lt15_cs0_num is not null,tmp1.txt_comment_lt15_cs0_num  ,0)            as txt_comment_lt15_cs0_num,
if(tmp1.txt_comment_lt15_cs1_num is not null,tmp1.txt_comment_lt15_cs1_num,0)              as txt_comment_lt15_cs1_num,
if(tmp1.txt_comment_lt15_cs2_num is not null,tmp1.txt_comment_lt15_cs2_num,0)              as txt_comment_lt15_cs2_num,
if(tmp1.txt_comment_ge15_cs0_num is not null,tmp1.txt_comment_ge15_cs0_num,0)              as txt_comment_ge15_cs0_num,
if(tmp1.txt_comment_ge15_cs1_num is not null,tmp1.txt_comment_ge15_cs1_num,0)              as txt_comment_ge15_cs1_num,
if(tmp1.txt_comment_ge15_cs2_num is not null,tmp1.txt_comment_ge15_cs2_num,0)              as txt_comment_ge15_cs2_num,
if(tmp1.sc_final_grade_0_num is not null,tmp1.sc_final_grade_0_num,0)                      as sc_final_grade_0_num,
if(tmp1.sc_final_grade_1_num is not null,tmp1.sc_final_grade_1_num,0)                      as sc_final_grade_1_num,
if(tmp1.sc_final_grade_2_num is not null,tmp1.sc_final_grade_2_num,0)                      as sc_final_grade_2_num,
if(tmp1.sc_final_grade_3_num is not null,tmp1.sc_final_grade_3_num,0)                      as sc_final_grade_3_num,
if(tmp1.sc_final_grade_4_num is not null,tmp1.sc_final_grade_4_num,0)                      as sc_final_grade_4_num,
if(tmp1.sc_final_grade_5_num is not null,tmp1.sc_final_grade_5_num,0)                      as sc_final_grade_5_num,
if(tmp1.sc_hastxt_comment_num is not null,tmp1.sc_hastxt_comment_num,0)                    as sc_hastxt_comment_num,
if(tmp1.sc_txt_comment_lt30_num is not null,tmp1.sc_txt_comment_lt30_num,0)                as sc_txt_comment_lt30_num,
if(tmp1.sc_txt_comment_ge30_num is not null,tmp1.sc_txt_comment_ge30_num,0)                as sc_txt_comment_ge30_num,
if(tmp1.nps_lt90_num is not null,tmp1.nps_lt90_num,0)                                      as nps_lt90_num,
if(tmp1.nps_ge90_num is not null,tmp1.nps_ge90_num,0)                                      as nps_ge90_num,
if(tmp1.txt_comment_lt30_nps_lt90_num is not null,tmp1.txt_comment_lt30_nps_lt90_num,0)    as txt_comment_lt30_nps_lt90_num,
if(tmp1.txt_comment_lt30_nps_gt90_num is not null,tmp1.txt_comment_lt30_nps_gt90_num,0)    as txt_comment_lt30_nps_gt90_num,
if(tmp1.txt_comment_ge30_nps_lt90_num is not null,tmp1.txt_comment_ge30_nps_lt90_num,0)    as txt_comment_ge30_nps_lt90_num,
if(tmp1.txt_comment_ge30_nps_gt90_num is not null,tmp1.txt_comment_ge30_nps_gt90_num,0)    as txt_comment_ge30_nps_gt90_num,
if(tmp1.sign_comment_num is not null,tmp1.sign_comment_num,0)                              as sign_comment_num,
if(tmp1.sign_comment_good_num is not null,tmp1.sign_comment_good_num,0)                    as sign_comment_good_num,
if(tmp1.sign_comment_nongood_num is not null,tmp1.sign_comment_nongood_num,0)              as sign_comment_nongood_num,
if(tmp1.cust_recommend_num is not null,tmp1.cust_recommend_num,0)                          as cust_recommend_num,
if(tmp1.cust_comp_num is not null,tmp1.cust_comp_num,0)                                    as cust_comp_num,                  
if(tmp1.first_called_report_shared_num is not null,tmp1.first_called_report_shared_num,0)  as first_called_report_shared_num, 
if(tmp1.see_reword_shared_num is not null,tmp1.see_reword_shared_num,0)                    as see_reword_shared_num,
current_timestamp()                                                                        as etl_time,
regexp_replace(${hiveconf:etl_date},'-','')                                                as pdate 

from (

select 
tmp.region                                         as region,
tmp.city_id                                        as city_id,
tmp.city_name                                      as city_name,
max(tmp.city_seq)                                  as city_seq,

sum(tmp.adjust_distribute_num)                     as adjust_distribute_num   , 
sum(tmp.first_distribute_num)                      as first_distribute_num,
sum(tmp.first_called_report_num)                   as first_called_report_num, 
sum(tmp.first_called_report_viewed_num)            as first_called_report_viewed_num, 
sum(tmp.see_report_num)                            as see_report_num, 
sum(tmp.see_report_viewed_num)                     as see_report_viewed_num,
sum(tmp.first_called_reword_num)                   as first_called_reword_num, 
sum(tmp.see_reword_num)                            as see_reword_num, 
sum(tmp.see_num)                                   as see_num, 
sum(tmp.sign_contains_cancel_ext_num)              as sign_contains_cancel_ext_num, 
sum(tmp.new_cust_follow_low_num)                   as new_cust_follow_low_num, 
sum(tmp.day_follow_low_num)                        as day_follow_low_num, 
sum(tmp.clue_close_nonsee_num)                     as clue_close_nonsee_num, 
sum(tmp.rv_final_grade_0_num)                      as rv_final_grade_0_num,
sum(tmp.rv_final_grade_1_num)                      as rv_final_grade_1_num,
sum(tmp.rv_final_grade_2_num)                      as rv_final_grade_2_num,
sum(tmp.rv_final_grade_3_num)                      as rv_final_grade_3_num,
sum(tmp.rv_final_grade_4_num)                      as rv_final_grade_4_num,
sum(tmp.rv_final_grade_5_num)                      as rv_final_grade_5_num,
sum(tmp.rv_hastxt_comment_num)                     as rv_hastxt_comment_num,
sum(tmp.rv_txt_comment_lt15_num)                   as rv_txt_comment_lt15_num,
sum(tmp.rv_txt_comment_ge15_num)                   as rv_txt_comment_ge15_num,
sum(tmp.continue_server_0_num)                     as continue_server_0_num,
sum(tmp.continue_server_1_num)                     as continue_server_1_num,
sum(tmp.continue_server_2_num)                     as continue_server_2_num,
sum(tmp.txt_comment_lt15_cs0_num)                  as txt_comment_lt15_cs0_num  ,
sum(tmp.txt_comment_lt15_cs1_num)                  as txt_comment_lt15_cs1_num,
sum(tmp.txt_comment_lt15_cs2_num)                  as txt_comment_lt15_cs2_num,
sum(tmp.txt_comment_ge15_cs0_num)                  as txt_comment_ge15_cs0_num,
sum(tmp.txt_comment_ge15_cs1_num)                  as txt_comment_ge15_cs1_num,
sum(tmp.txt_comment_ge15_cs2_num)                  as txt_comment_ge15_cs2_num,
sum(tmp.sc_final_grade_0_num)                      as sc_final_grade_0_num,
sum(tmp.sc_final_grade_1_num)                      as sc_final_grade_1_num,
sum(tmp.sc_final_grade_2_num)                      as sc_final_grade_2_num,
sum(tmp.sc_final_grade_3_num)                      as sc_final_grade_3_num,
sum(tmp.sc_final_grade_4_num)                      as sc_final_grade_4_num,
sum(tmp.sc_final_grade_5_num)                      as sc_final_grade_5_num,
sum(tmp.sc_hastxt_comment_num)                     as sc_hastxt_comment_num,
sum(tmp.sc_txt_comment_lt30_num)                   as sc_txt_comment_lt30_num,
sum(tmp.sc_txt_comment_ge30_num)                   as sc_txt_comment_ge30_num,
sum(tmp.nps_lt90_num)                              as nps_lt90_num,
sum(tmp.nps_ge90_num)                              as nps_ge90_num,
sum(tmp.txt_comment_lt30_nps_lt90_num)             as txt_comment_lt30_nps_lt90_num,
sum(tmp.txt_comment_lt30_nps_gt90_num)             as txt_comment_lt30_nps_gt90_num,
sum(tmp.txt_comment_ge30_nps_lt90_num)             as txt_comment_ge30_nps_lt90_num,
sum(tmp.txt_comment_ge30_nps_gt90_num)             as txt_comment_ge30_nps_gt90_num,
sum(tmp.sign_comment_num)                          as sign_comment_num,
sum(tmp.sign_comment_good_num)                     as sign_comment_good_num,
sum(tmp.sign_comment_nongood_num)                  as sign_comment_nongood_num,
sum(tmp.cust_recommend_num)                        as cust_recommend_num,
sum(tmp.cust_comp_num)                             as cust_comp_num,                  
sum(tmp.first_called_report_shared_num)            as first_called_report_shared_num, 
sum(tmp.see_reword_shared_num)                     as see_reword_shared_num 

from (

select 
t16.region     as region,

--城市id，名称，带开城顺序的城市名称
case 
when t1.city_id is not null and t1.city_id not in (0,-1) then t1.city_id 
when t2.city_id is not null and t2.city_id not in (0,-1) then t2.city_id 
when t3.city_id is not null and t3.city_id not in (0,-1) then t3.city_id 
when t4.city_id is not null and t4.city_id not in (0,-1) then t4.city_id 
when t5.city_id is not null and t5.city_id not in (0,-1) then t5.city_id 
when t6.city_id is not null and t6.city_id not in (0,-1) then t6.city_id 
when t7.city_id is not null and t7.city_id not in (0,-1) then t7.city_id 
when t8.city_id is not null and t8.city_id not in (0,-1) then t8.city_id 
when t9.city_id is not null and t9.city_id not in (0,-1) then t9.city_id 
when t10.city_id is not null and t10.city_id not in (0,-1) then t10.city_id 
when t11.city_id is not null and t11.city_id not in (0,-1) then t11.city_id 
when t12.city_id is not null and t12.city_id not in (0,-1) then t12.city_id 
when t13.city_id is not null and t13.city_id not in (0,-1) then t13.city_id 
when t14.city_id is not null and t14.city_id not in (0,-1) then t14.city_id 
when t15.city_id is not null and t15.city_id not in (0,-1) then t15.city_id 
when t16.city_id is not null and t16.city_id not in (0,-1) then t16.city_id 
else 0 end                                                                      as city_id,
t16.mgr_city_name                                                               as city_name,
t16.mgr_city_seq                                                                as city_seq, 

--指标
t1.adjust_distribute_num, -- 上户核算量
t2.first_distribute_num, -- 首次分配上户量
t3.first_called_report_num, -- 发送首电报告数量
t3.first_called_report_viewed_num, -- 首电报告被浏览数量 
t4.first_see_report_num as see_report_num, -- 发送首电报告数量
t4.first_see_report_viewed_num as see_report_viewed_num, -- 首电报告被浏览数量
t5.first_called_reword_num, -- 首电报告被打赏数量 
t6.see_reword_num, -- 带看被打赏数量
t7.see_num, -- 带看量
t8.sign_contains_cancel_ext_num, -- 签约量-含退-含外联
t9.new_cust_follow_low_num, -- 新上户跟进频率低数量
t9.day_follow_low_num, -- 日常维护频率太低数量 
t10.clue_close_nonsee_num, -- 未带看关闭线索数量
t11.rv_final_grade_0_num,
t11.rv_final_grade_1_num,
t11.rv_final_grade_2_num,
t11.rv_final_grade_3_num,
t11.rv_final_grade_4_num,
t11.rv_final_grade_5_num,
t11.rv_hastxt_comment_num,
t11.rv_txt_comment_lt15_num,
t11.rv_txt_comment_ge15_num,
t11.continue_server_0_num,
t11.continue_server_1_num,
t11.continue_server_2_num,
t11.txt_comment_lt15_cs0_num,
t11.txt_comment_lt15_cs1_num,
t11.txt_comment_lt15_cs2_num,
t11.txt_comment_ge15_cs0_num,
t11.txt_comment_ge15_cs1_num,
t11.txt_comment_ge15_cs2_num,
t12.sc_final_grade_0_num,
t12.sc_final_grade_1_num,
t12.sc_final_grade_2_num,
t12.sc_final_grade_3_num,
t12.sc_final_grade_4_num,
t12.sc_final_grade_5_num,
t12.sc_hastxt_comment_num,
t12.sc_txt_comment_lt30_num,
t12.sc_txt_comment_ge30_num,
t12.nps_lt90_num,
t12.nps_ge90_num,
t12.txt_comment_lt30_nps_lt90_num,
t12.txt_comment_lt30_nps_gt90_num,
t12.txt_comment_ge30_nps_lt90_num,
t12.txt_comment_ge30_nps_gt90_num,
t13.sign_comment_num,
t13.sign_comment_good_num,
t13.sign_comment_nongood_num,
t14.cust_recommend_num,
t15.cust_comp_num,
0                                                  as first_called_report_shared_num,
0                                                  as see_reword_shared_num

from tmp_adjust_distribute        t1 
full join tmp_first_distribute    t2  on t1.city_id = t2.city_id 
full join tmp_called_first_report t3  on t1.city_id = t3.city_id 
full join tmp_see_first_report    t4  on t1.city_id = t4.city_id 
full join tmp_first_called_reword t5  on t1.city_id = t5.city_id 
full join tmp_see_reword          t6  on t1.city_id = t6.city_id 
full join tmp_see_project         t7  on t1.city_id = t7.city_id 
full join tmp_sign                t8  on t1.city_id = t8.city_id 
full join tmp_cust_follow         t9  on t1.city_id = t9.city_id 
full join tmp_clue_close_nonsee   t10 on t1.city_id = t10.city_id
full join tmp_return_visit        t11 on t1.city_id = t11.city_id
full join tmp_see_comment         t12 on t1.city_id = t12.city_id
full join tmp_sign_comment        t13 on t1.city_id = t13.city_id
full join tmp_cust_recommend      t14 on t1.city_id = t14.city_id
full join tmp_customer_complaints t15 on t1.city_id = t15.city_id

left join tmp_city t16 on case 
when t1.city_id is not null and t1.city_id not in (0,-1) then t1.city_id 
when t2.city_id is not null and t2.city_id not in (0,-1) then t2.city_id 
when t3.city_id is not null and t3.city_id not in (0,-1) then t3.city_id 
when t4.city_id is not null and t4.city_id not in (0,-1) then t4.city_id 
when t5.city_id is not null and t5.city_id not in (0,-1) then t5.city_id 
when t6.city_id is not null and t6.city_id not in (0,-1) then t6.city_id 
when t7.city_id is not null and t7.city_id not in (0,-1) then t7.city_id 
when t8.city_id is not null and t8.city_id not in (0,-1) then t8.city_id 
when t9.city_id is not null and t9.city_id not in (0,-1) then t9.city_id 
when t10.city_id is not null and t10.city_id not in (0,-1) then t10.city_id 
when t11.city_id is not null and t11.city_id not in (0,-1) then t11.city_id 
when t12.city_id is not null and t12.city_id not in (0,-1) then t12.city_id 
when t13.city_id is not null and t13.city_id not in (0,-1) then t13.city_id 
when t14.city_id is not null and t14.city_id not in (0,-1) then t14.city_id 
when t15.city_id is not null and t15.city_id not in (0,-1) then t15.city_id  
else 0 end = t16.city_id 

)tmp 
group by tmp.region,tmp.city_id,tmp.city_name

) tmp1
;



