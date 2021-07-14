
drop table if exists julive_fact.fact_campus_candidate_recruit_dtl;
create table julive_fact.fact_campus_candidate_recruit_dtl(
order_id                                  bigint   COMMENT '校园招聘id' ,
name                                      string   COMMENT '候选人姓名',
      
post_type                                 int      COMMENT '职位类型 1咨询师', 
--面试过程      
      
--面试活动
active_id                                 int      COMMENT '场次id',      
campus_recruitment_name                   string   COMMENT '校招活动场次名称', 
campus_recruitment_city                   int      COMMENT '校招城市', 
campus_recruitment_university             int      COMMENT '校招大学',


session_principal                         string   COMMENT '场次负责人', 
active_start_date                         string   COMMENT '校招活动开始时间',  
active_end_date                           string   COMMENT '校招活动结束时间',    
      
--简历筛选      
first_filtrate_interviewer                int    COMMENT '初筛面试官',
native_place                              string COMMENT '生源地（籍贯）', 
target_industry                           string COMMENT '目标行业', 
target_salary                             string COMMENT '目标薪水', 
target_post                               string COMMENT '目标岗位', 
is_often_offjob                           int    COMMENT '是否频繁更换工作 0未知 1是 2否', 
is_past_job_intensive                     int    COMMENT '以往工作是否有高强度的经历 0未知 1 是 2 否', 
is_marketing_experience                   int    COMMENT '是否有过销售工作经验 0未知 1 是 2 否', 
is_big_house_enterprise                   int    COMMENT '是否有大型房企开发经历 0未知 1 是 2否', 
is_good_tube_pearson                      int    COMMENT '是否有过知名企业管培生经历 0未知 1 是 2否', 
is_own_business                           int    COMMENT '是否有过创业经历 0未知 1 是 2 否', 
is_student_work                           int    COMMENT '是否有过实习经历( 0未知 1 是 2否)', 
is_student_cadres                         int    COMMENT '是否有过学生干部经历 0未知 1 就 2 否', 
is_outreach_event                         int    COMMENT '是否有过外联活动经历 0未知 1 是 2否', 
is_debate_experience                      int    COMMENT '是否有过辩论/演讲经历 0未知 1. 是 2 否', 

   

--笔试
write_interview_date                      int    COMMENT '面试时间', 
write_score                               string COMMENT '面试得分', 
write_is_pass                             int    COMMENT '面试结果 1通过 2未通过', 
write_p_id                                int    COMMENT '配置模板id', 
write_update_date                         int    COMMENT '更新时间', 
write_create_date                         int    COMMENT '创建时间', 
write_creator                             int    COMMENT '创建者', 
write_updator                             int    COMMENT '更新者', 
write_form_data                           string COMMENT '自定义表单数据', 
write_p_ap                                double COMMENT 'p_ap 得分', 
write_m_ap                                double COMMENT 'm_ap得分', 
write_m_a                                 double COMMENT 'm_a得分', 
write_p_a                                 double COMMENT 'p_a 得分', 
write_basic_score                         double COMMENT '基础测试题得分', 
write_knowledge_score                     double COMMENT '企业知识题得分',
 --群面
group_hr                                  int    COMMENT '群面面试官', 
group_interview_date                      int    COMMENT '面试时间', 
group_interview_group                     string COMMENT '群面小组', 
group_score                               double COMMENT '得分', 
group_is_pass                             int    COMMENT '是否通过 1是 2否', 
group_evaluate                            string COMMENT '面试评价', 
group_tmp_config_id                       int    COMMENT '模板配置id', 
group_form_data                           string COMMENT '自定义表单数据',
 --终面
final_hr                                  int    COMMENT '终面面试官', 
final_interview_date                      int    COMMENT '面试时间', 
final_score                               string COMMENT '面试得分', 
final_is_pass                             int    COMMENT '面试结果 1通过 2未通过', 
final_interview_assessment                string COMMENT '综合面试评价', 
final_p_id                                int    COMMENT '配置模板id', 
final_update_date                         int    COMMENT '更新时间', 
final_create_date                         int    COMMENT '创建时间', 
--是否发offer    
offer_is_pass                             int    COMMENT 'offer审批通过状态 1是 2否', 
offer_approval_date                       int    COMMENT 'offer审批时间', 
offer_address                             string COMMENT '公司邮件地址', 
offer_is_send_email                       int    COMMENT '邮件是否发送 1是 2否', 
offer_cause_close_type                    int    COMMENT '关闭订单原因', 
offer_cause_close                         string COMMENT '关闭原因补充说明',  

--offer跟进
employ_follow                             int    COMMENT '录用跟进人',                   
offer_follow_num                          int    COMMENT '已录入offer跟进记录次数', 
entry_follow_num                          int    COMMENT '已录入入职跟进记录次数', 

              
etl_time                                  string COMMENT '跑数时间'       
)
COMMENT '校招招聘事实表'

stored as parquet
;        
