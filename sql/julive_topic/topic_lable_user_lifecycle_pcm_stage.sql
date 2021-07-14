set hive.execution.engine=spark;
set spark.app.name=topic_lable_user_lifecycle_pcm_stage;
set spark.yarn.queue=etl;
insert overwrite table julive_topic.topic_lable_user_lifecycle_pcm_stage
select 
t1.global_id,        
if(t1.distribute_date_min is null and t1.is_30day_live =1 and t1.total_points <=6.0 ,'101',
  if(t1.distribute_date_min is null  and t1.is_30day_live =1 and t1.total_points >6.0 and t1.total_points <=14.0,'102',
    if(t1.distribute_date_min is null and t1.is_30day_live =1 and t1.total_points >14.0,'103',
      if(t1.distribute_date_min is not null and t1.is_30day_live =1 and t1.first_subscribe_date_min is null and t1.total_points<=19.5,'201',
        if(t1.distribute_date_min is not null and t1.is_30day_live =1 and t1.first_subscribe_date_min is null and t1.total_points>19.5,'202',
          if(t1.distribute_date_min is not null and t1.is_30day_live =0 and t1.first_subscribe_date_min is null and t1.total_points<=19.5,'301',
            if(t1.distribute_date_min is not null and t1.is_30day_live =0 and t1.first_subscribe_date_min is null and t1.total_points>19.5,'302',
             if(t1.first_subscribe_date_min is not null,'401','402'
           ))))))))

from
julive_topic.topic_lable_user_lifecycle_pcm_rating t1;

