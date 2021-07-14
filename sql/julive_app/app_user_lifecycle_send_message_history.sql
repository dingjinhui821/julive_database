set hive.execution.engine=spark;
set spark.app.name=app_user_lifecycle_send_message_history;
set mapred.job.name=app_user_lifecycle_send_message_history;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

with send_message_base_history as(

SELECT 
t.global_id,
t.julive_id,
t.send_source,
t.user_mobile
from
   ( SELECT 
    global_id,
    julive_id,
    send_source,
    user_mobile,
    row_number() over(partition by user_mobile order by global_id desc) as rn
    from julive_app.app_user_lifecycle_send_message_base
    WHERE user_mobile is NOT NULL
    AND user_mobile != ''
    AND julive_id IS NOT NULL
    AND julive_id != ''
)t
WHERE t.rn = 1
),

topic_lable_user_lifecycle_base_sent as(

select 
julive_id,
max(user_mobile)              as user_mobile,
max(first_visit_daytime)      as first_visit_daytime,
max(create_date_min)          as create_date_min,
max(distribute_date_max)      as distribute_date_max,
max(first_see_date_max)       as first_see_date_max,
max(first_subscribe_date_max) as first_subscribe_date_max,
max(first_sign_date_max)      as first_sign_date_max
from 
julive_topic.topic_lable_user_lifecycle_base
where first_subscribe_date_max<'2020-09-06'
or first_subscribe_date_max>'2020-10-22'
or first_subscribe_date_max is null
group by julive_id

),
lable_user_lifecycle_intelligent as(
select 
julive_id,
max(user_group) as user_group
from 
tmp_select_user.dwd_jdp_intelligent_touch_effort_msg_dtl_daily 
where 
pdate='20201023'
AND batch = '202010231549'
group by julive_id
)

insert overwrite table julive_app.app_user_lifecycle_send_message_history partition(pdate)
select 

t1.global_id,
t1.julive_id,
t1.send_source,
t1.user_mobile,
t3.user_group,
t2.first_visit_daytime,     
t2.create_date_min,         
t2.distribute_date_max,     
t2.first_see_date_max,      
t2.first_subscribe_date_max,
t2.first_sign_date_max,
regexp_replace(date_add(current_date(),-1),'-','') as pdate     

from send_message_base_history t1
left join topic_lable_user_lifecycle_base_sent t2 on t1.julive_id = t2.julive_id
LEFT JOIN  lable_user_lifecycle_intelligent t3 ON t1.julive_id = t3.julive_id
;



