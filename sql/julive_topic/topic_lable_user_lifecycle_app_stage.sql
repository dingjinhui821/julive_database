set hive.execution.engine=spark;
set spark.app.name=topic_lable_user_lifecycle_app_stage;
set spark.yarn.queue=etl;
with topic_lable_user_lifecycle_app_rating_tmp as(
select 
global_id,
max(t1.is_7day_live)                  as is_7day_live,                          
max(t1.is_30day_live)                 as is_30day_live,  
min(t1.create_date_min)               as create_date_min,         
max(t1.create_date_max)               as create_date_max,         
min(t1.distribute_date_min)           as distribute_date_min,     
max(t1.distribute_date_max)           as distribute_date_max,     
min(t1.first_see_date_min)            as first_see_date_min,      
max(t1.first_see_date_max)            as first_see_date_max,      
min(t1.first_subscribe_date_min)      as first_subscribe_date_min,
max(t1.first_subscribe_date_max)      as first_subscribe_date_max,
min(t1.first_sign_date_min)           as first_sign_date_min,     
max(t1.first_sign_date_max)           as first_sign_date_max, 
max(total_points)                     as total_points

from 
julive_topic.topic_lable_user_lifecycle_app_rating t1
group by global_id

)

insert overwrite table julive_topic.topic_lable_user_lifecycle_app_stage
select 
t1.global_id,        
if(t1.distribute_date_min is null and t1.is_30day_live =1 and t1.total_points <4.5 ,'101',
  if(t1.distribute_date_min is null  and t1.is_30day_live =1 and t1.total_points >=4.5 and t1.total_points <20.3,'102',
    if(t1.distribute_date_min is null and t1.is_30day_live =1 and t1.total_points >=20.3,'103',
      if(t1.distribute_date_min is not null and t1.is_30day_live =1 and t1.first_subscribe_date_min is null and t1.total_points<27.0,'201',
        if(t1.distribute_date_min is not null and t1.is_30day_live =1 and t1.first_subscribe_date_min is null and t1.total_points>=27.0,'202',
          if(t1.distribute_date_min is not null and t1.is_30day_live =0 and t1.first_subscribe_date_min is null and t1.total_points<19.5,'301',
            if(t1.distribute_date_min is not null and t1.is_30day_live =0 and t1.first_subscribe_date_min is null and t1.total_points>=19.5,'302',
             if(t1.first_subscribe_date_min is not null,'401','402'
           )))))))) as user_stage

from
topic_lable_user_lifecycle_app_rating_tmp t1;



