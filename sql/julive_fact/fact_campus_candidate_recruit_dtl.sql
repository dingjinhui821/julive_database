set hive.execution.engine=spark;
set spark.app.name=fact_campus_candidate_recruit_dtl;
set spark.yarn.queue=etl;

insert overwrite table julive_fact.fact_campus_candidate_recruit_dtl

select 

t1.id                                            as order_id,                   
t2.name,                       
           
t1.post_type,                  
--面试过程
--面试活动
t1.active_id,                  

t3.campus_recruitment_name,      
t3.campus_recruitment_city,      
t3.campus_recruitment_university,
  
t3.session_principal,          
from_unixtime(t3.active_start_time,'yyyy-MM-dd') as active_start_date,          
from_unixtime(t3.active_end_time,'yyyy-MM-dd')   as active_end_date,            

--简历筛选
t1.first_filtrate_interviewer, 
t4.native_place,               
t4.target_industry,            
t4.target_salary,              
t4.target_post  ,              
t4.is_often_offjob ,           
t4.is_past_job_intensive,      
t4.is_marketing_experience,   
t4.is_big_house_enterprise,    
t4.is_good_tube_pearson,       
t4.is_own_business,            
t4.is_student_work,            
t4.is_student_cadres,          
t4.is_outreach_event,          
t4.is_debate_experience,       

--笔试
from_unixtime(t5.interview_time,'yyyy-MM-dd')       as write_interview_date,             
t5.score                                            as write_score,                      
t5.is_pass                                          as write_is_pass,                    
t5.p_id                                             as write_p_id,                       
from_unixtime(t5.update_datetime,'yyyy-MM-dd')      as write_update_date,            
from_unixtime(t5.create_datetime,'yyyy-MM-dd')      as write_create_date,            
t5.creator                                          as write_creator        ,                    
t5.updator                                          as write_updator        ,                    
t5.form_data                                        as write_form_data      ,                  
t5.p_ap                                             as write_p_ap           ,                       
t5.m_ap                                             as write_m_ap           ,                      
t5.m_a                                              as write_m_a            ,                        
t5.p_a                                              as write_p_a            ,                        
t5.basic_score                                      as write_basic_score    ,                
t5.knowledge_score                                  as write_knowledge_score,            
 --群面
t1.group_hr,                   
from_unixtime(t6.interview_time,'yyyy-MM-dd')       as group_interview_date,             
t6.interview_group                                  as group_interview_group,            
t6.score                                            as group_score,                      
t6.is_pass                                          as group_is_pass,                    
t6.evaluate                                         as group_evaluate,                   
t6.tmp_config_id                                    as group_tmp_config_id,              
t6.form_data                                        as group_form_data,                  
 --终面  
t1.final_hr,                     
from_unixtime(t7.interview_time,'yyyy-MM-dd')       as final_interview_date,             
t7.score                                            as final_score,                      
t7.is_pass                                          as final_is_pass,                    
t7.interview_assessment                             as final_interview_assessment,       
t7.p_id                                             as final_p_id,                       
from_unixtime(t7.update_datetime,'yyyy-MM-dd')      as final_update_date,            
from_unixtime(t7.create_datetime,'yyyy-MM-dd')      as final_create_date,            
--是否发offer      
t8.is_pass                                          as offer_is_pass,                    
from_unixtime(t8.approval_time,'yyyy-MM-dd')        as offer_approval_date,              
t8.address                                          as offer_address         ,                    
t8.is_send_email                                    as offer_is_send_email   ,              
t8.cause_close_type                                 as offer_cause_close_type,           
t8.cause_close                                      as offer_cause_close     ,  
--offer跟进
t1.employ_follow,               
t1.offer_follow_num,           
t1.entry_follow_num,           

current_timestamp()                                 as etl_time                   

from ods.zp_campus_order t1 
left join ods.zp_candidate_info t2 on t1.id =t2.order_id
left join ods.zp_campus_active t3 on t1.active_id = t3.id
left join ods.zp_cv_filter_quality t4 on t1.id = t4.order_id
left join ods.zp_campus_write_exam_result t5 on t1.id = t5.order_id
left join ods.zp_campus_group_interview_result t6 on t1.id = t6.order_id
left join ods.zp_campus_final_result t7 on t1.id = t7.order_id
left join ods.zp_campus_offer_issue_record t8 on t1.id = t8.order_id
;


