drop table if exists ods.zp_phone_interview_quality;
create external table ods.zp_phone_interview_quality(
id                                                     int             comment '',
order_id                                               bigint          comment '订单id',
is_accept_sales_work                                   int             comment '是否接受销售工作(0 未知 1 是 2 否 3 未考察)',
is_accept_overtime                                     int             comment '是否接受加班(0 未知 1 是 2 否 3 未考察)',
is_accept_no_work_day                                  int             comment '是否接受调休制 (0 未知 1 是 2 否 3 未考察)',
is_communicate_ok                                      int             comment '沟通是否明显停顿，说话流畅 (0 未知 1 是 2 否 3 未考察)',
is_accept_salary                                       int             comment '是否接受岗位薪资 (0 未知 1 是 2 否 3 未考察)',
is_unify_enrolment                                     int             comment '第一学历是否为统招三本及以上 (0 未知 1 是 2 否 3 未考察)',
is_have_relevant_experience                            int             comment '是否有销售或房企工作经验(0 未知 1 是 2 否 3 未考察)',
is_have_overtime_history                               int             comment '是否有加班经历 (0 未知 1 是 2 否 3 未考察)',
is_211school_upper                                     int             comment '是否毕业于211及以上院校 (0 未知 1 是 2 否 3 未考察)',
is_know_company_post                                   int             comment '是否了解公司及岗位 (0 未知 1 是 2 否 3 未考察)',
is_high_job_intention                                  int             comment '是否有高求职意向 (0 未知 1 是 2 否 3 未考察)',
before_work_rank                                       double          comment '过往工作业绩排名',
school_rank                                            double          comment '在校学习成绩排名',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/zp_phone_interview_quality'
;
