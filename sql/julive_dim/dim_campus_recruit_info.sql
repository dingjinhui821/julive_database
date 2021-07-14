set hive.execution.engine=spark;
set spark.app.name=dim_campus_recruit_info;
set spark.yarn.queue=etl;

insert overwrite table julive_dim.dim_campus_recruit_info 

select 
t1.id as order_id,

t2.name,
t2.sex,
t2.birthday ,
t2.tel,
t2.spare_tel,  
t2.email,
t2.spare_email,
t2.politics_status,
t2.native_place,   
t2.now_address,    

t2.school_id,
t10.school_name,
t10.school_type,
t2.education_level,
t4.profession_category,
t4.profession_name,    
t4.start_edu_time,     
t4.end_edu_time,       

t4.class_ranking,
t5.is_student_cadres,
t2.english_level,
t2.english_score,

t1.post_type,


t9.is_send_email,
if (t9.is_send_email=1 ,t9.update_datetime,'') as sendoffer_update_datetime,

current_timestamp() as etl_time


from ods.zp_campus_order t1
left join ods.zp_candidate_info t2 on t1.id = t2.order_id

left join (select 
              tmp.order_id,
              tmp.class_ranking,
              tmp.start_edu_time,
              tmp.end_edu_time,
              tmp.profession_category,
              tmp.profession_name
     
              from
              (
              select 
              order_id,
              class_ranking,
              start_edu_time,
              end_edu_time,
              profession_category,
              profession_name,
              row_number() over(partition by order_id order by end_edu_time desc) as rn
              from
              ods.zp_candidate_edu) tmp
              where tmp.rn =1) t4 on t2.order_id = t4.order_id

left join ods.zp_cv_filter_quality t5 on t1.id = t5.order_id

left join ods.zp_campus_offer_issue_record t9 on t1.id = t9.order_id

left join ods.zp_school_info t10 on t10.id  = t2.school_id
;

