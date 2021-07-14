
-- 核心事件标签表 
-- ---------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------
-- 依赖 
-- julive_fact.fact_event_track_agg 
-- julive_portrait.portrait_lable_summary_user 
-- julive_fact.fact_global_bus_indi 

-- 核心事件 
-- e_click_project_card
-- e_click_house_type_card
-- e_click_search_entry
-- e_filter_project


-- 1 schema 
-- drop table if exists tmp_dev_1.tmp_userlife_core_event_base;
-- create table tmp_dev_1.tmp_userlife_core_event_base(
-- 
-- global_id          string,
-- product_id         string,
-- event              string,
-- frompage           string,
-- topage             string,
-- pv                 string 
-- 
-- ) partitioned by (pdate string)
-- stored as parquet 
-- ;
-- 



-- 3 etl 逻辑 
set etl_date = '${hiveconf:etlDate}'; 
set spark.app.name=topic_lable_user_lifecycle_base;
set mapred.job.name=topic_lable_user_lifecycle_base;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
-- 抽取核心事件临时表 
insert into table tmp_dev_1.tmp_userlife_core_event_base partition(pdate = ${hiveconf:etl_date}) 

select 

global_id,
product_id,
event,
frompage,
topage,
pv

from julive_fact.fact_event_track_agg t1 
where 1 = 1 
and pdate = ${hiveconf:etl_date} 
and global_id is not null 
and t1.event in (
'e_click_project_card',
'e_click_house_type_card',
'e_click_search_entry',
'e_filter_project'
)
;


-- 每个事件 每个人的日均值 
drop table if exists tmp_dev_1.tmp_global_core_event_avg_indi;
create table tmp_dev_1.tmp_global_core_event_avg_indi as 

select 

global_id,

sum(if(event = 'e_click_project_card',pv,0)) / count(distinct if(event = 'e_click_project_card',pdate,null)) as pc_day_avg, -- 点击楼盘卡日均点击片量 
sum(if(event = 'e_click_house_type_card',pv,0)) / count(distinct if(event = 'e_click_house_type_card',pdate,null)) as htc_day_avg, -- 点击户型卡片日均点击量 
sum(if(event = 'e_click_search_entry',pv,0)) / count(distinct if(event = 'e_click_search_entry',pdate,null)) as se_day_avg, -- 搜索入口日均点击量 
sum(if(event = 'e_filter_project',pv,0)) / count(distinct if(event = 'e_filter_project',pdate,null)) as fp_day_avg -- 过滤楼盘日均点击量 

from tmp_dev_1.tmp_userlife_core_event_base t1 
group by global_id 
;


-- 每个人 每个事件最大连续登陆天数 
drop table if exists tmp_dev_1.tmp_global_core_event_cont_day_indi;
create table tmp_dev_1.tmp_global_core_event_cont_day_indi as 

select  

global_id,

max(if(event = 'e_click_project_card',pv,0)) as cp_cont_pv,
max(if(event = 'e_click_project_card',continue_days,0)) as cp_cont_days,

max(if(event = 'e_click_house_type_card',pv,0)) as htc_cont_pv,
max(if(event = 'e_click_house_type_card',continue_days,0)) as htc_cont_days,

max(if(event = 'e_click_search_entry',pv,0)) as se_cont_pv,
max(if(event = 'e_click_search_entry',continue_days,0)) as se_cont_days,

max(if(event = 'e_filter_project',pv,0)) as fp_cont_pv,
max(if(event = 'e_filter_project',continue_days,0)) as fp_cont_days 

from ( 
select 

tmp.global_id,
tmp.event,
max(tmp.continue_days) as continue_days,
split(max(concat(tmp.continue_days,'|',tmp.pv)),'\\|')[1] as pv 


from (

select 

global_id,
event,
(continue_level - rn) as level,

sum(pv) as pv,
count(1) as continue_days 

from (

select 

global_id,
event,
pdate,

datediff(concat_ws('-',substr(pdate,1,4),substr(pdate,5,2),substr(pdate,7,2)),'2019-01-01') as continue_level,
row_number()over(partition by global_id,event order by pdate) as rn,

sum(pv) as pv 

from tmp_dev_1.tmp_userlife_core_event_base t1 
group by global_id,event,pdate 

) t 
group by global_id,event,(continue_level - rn) -- 每个事件持续天数 

) tmp 

group by tmp.global_id ,tmp.event 
) t 
group by global_id 
;



-- global核心指标汇总表 
drop table if exists tmp_dev_1.tmp_global_core_event_result;
create table tmp_dev_1.tmp_global_core_event_result as 

select 

tmp1.global_id,

tmp1.pc_day_avg,
tmp1.htc_day_avg,
tmp1.se_day_avg,
tmp1.fp_day_avg,

tmp2.cp_cont_pv,
tmp2.cp_cont_days,
tmp2.htc_cont_pv,
tmp2.htc_cont_days,
tmp2.se_cont_pv,
tmp2.se_cont_days,
tmp2.fp_cont_pv,
tmp2.fp_cont_days 

from tmp_dev_1.tmp_global_core_event_avg_indi tmp1 
left join tmp_dev_1.tmp_global_core_event_cont_day_indi tmp2 on tmp1.global_id = tmp2.global_id 
;



-- ---------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------
-- 用户生命周期底表 
insert overwrite table julive_topic.topic_lable_user_lifecycle_base  

select 

-- 基础标签 
t1.global_id                    ,
t1.product_id                   ,
t1.comjia_unique_id             ,
if(t1.julive_id is not null, t1.julive_id,t2.julive_id) as julive_id,
t2.user_name                    ,
if(t2.user_mobile is not null,t2.user_mobile,t4.user_mobile) as user_mobile,
t2.emp_id                       , 
t2.emp_name                     , 
t1.ip_city                      ,
t1.channel_id                   ,
t1.select_city                  ,
t1.manufacturer,
t1.app_version ,
t1.os            as os_system   ,
t1.os_version  ,
t1.is_close,
t1.latest_distribute_date,
t1.comjia_unique_id_valid_status,                
t1.comjia_unique_id_valid_status_update_datetime,

-- 行为标签 
t1.first_visit_daytime          ,
t1.final_visit_daytime          ,
t1.visit_day_cnt                ,
t1.day_diff_from_last_visit     ,
t1.is_7day_live                 ,
t1.is_30day_live                ,
t1.visit_days_cnt_last_30day    ,
t1.visit_session_cnt_last_30day ,
t1.visit_duration_last_30day    ,
t1.visit_cnt_per_day            ,
t1.visit_duration_per_day       ,
t1.frequent_visit_hour_1st      ,
t1.frequent_visit_hour_2nd      ,
t1.frequent_visit_hour_3rd      ,

-- 偏好标签 
t1.fav_project                  ,
t1.fav_house_type               ,
t1.district_name                ,
t1.project_type                 ,
t1.order_house_type             ,
t1.order_project_type           , 

if(t2.create_date_min is not null,1,0)               as is_gen_clue, -- user产生线索标识 1-是 0-否  -- 
if(t2.distribute_date_min is not null,1,0)           as is_distribute, -- user被分配上户标识 1-是 0-否 
if(t2.first_see_date_min is not null,1,0)            as is_see, -- user带看标识 1-是 0-否 
if(t2.first_subscribe_date_min is not null,1,0)      as is_subscribe, -- user认购标识 1-是 0-否 
if(t2.first_sign_date_min is not null,1,0)           as is_sign, -- user签约标识 1-是 0-否 

-- 统计标签 
t1.video_play_cnt               ,
t1.video_play_duration          ,
t1.call_video_cnt               ,
t1.call_video_duration          ,
t1.activity_cnt                 ,
t1.qa_ask_cnt                   ,
t1.qa_click_cnt                 ,
t1.qa_view_duration             ,
t1.info_article_cnt             ,
t1.info_video_cnt               ,
t1.info_project_cnt             ,
t1.info_question_cnt            ,
t1.leave_phone_cnt              ,
t1.service_chat_cnt             ,
t1.service_chat_duration        ,
t1.booking_cnt                  ,
t1.submit_question_cnt          ,
t1.project_comment_cnt          ,
t1.send_comment_cnt             ,
t1.share_cnt                    ,
t1.like_cnt                     ,
t1.love_cnt                     ,
t1.follow_cnt                   ,
t1.collect_cnt                  ,
t1.query_cnt                    ,
t1.view_cnt                     ,

t1.real_estate_cnt              ,  
t1.real_estate_list             ,
t1.house_decoration_cnt         ,
t1.house_decoration_list        ,
t1.car_cnt                      ,
t1.car_list                     ,
t1.marriage_cnt                 ,
t1.marriage_list                ,
t1.mombaby_list                 ,
t1.mombaby_cnt                  ,
t1.investment_list              ,
t1.investment_cnt               ,

t1.order_status                 ,
t1.order_decision               ,
t1.order_score                  ,
t1.order_urgent                 ,
t1.order_district_id            ,
t1.order_pref_zone              ,

t1.order_interest_project       ,
t1.order_interest_project_name  ,
t1.order_recommend_project      ,
t1.order_recommend_project_name ,

t4.first_register_date          ,
t2.create_date_min              ,
t2.create_date_max              ,
t2.distribute_date_min          ,
t2.distribute_date_max          ,
t2.first_see_date_min           ,
t2.first_see_date_max           ,
t2.first_subscribe_date_min     ,
t2.first_subscribe_date_max     ,
t2.first_sign_date_min          ,
t2.first_sign_date_max          ,
t2.clue_num                     ,
t2.distribute_num               ,
t2.see_num                      ,
t2.see_project_num              ,
t2.subscribe_num                ,
t2.sign_num                     ,
t2.call_duration                ,
t2.call_num                     ,
t2.clue_see_num                 ,
t2.clue_subscribe_num           ,
t2.clue_sign_num                ,

t3.pc_day_avg                   ,
t3.htc_day_avg                  ,
t3.se_day_avg                   ,
t3.fp_day_avg                   ,
t3.cp_cont_pv                   ,
t3.cp_cont_days                 ,
t3.htc_cont_pv                  ,
t3.htc_cont_days                ,
t3.se_cont_pv                   ,
t3.se_cont_days                 ,
t3.fp_cont_pv                   ,
t3.fp_cont_days                 


from julive_portrait.portrait_lable_summary_user t1 
left join julive_fact.fact_global_bus_indi t2 on t1.global_id = t2.global_id 
left join tmp_dev_1.tmp_global_core_event_result t3 on t1.global_id = t3.global_id 
left join julive_fact.fact_global_register_no_phone_numeber_dtl t4 on t1.global_id = t4.global_id and coalesce(t1.julive_id,t2.julive_id) = t4.julive_id
;


