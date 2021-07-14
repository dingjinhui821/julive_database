drop table if exists ods.zp_person_second_interview_result;
create external table ods.zp_person_second_interview_result(
id                                                     int             comment '',
order_id                                               bigint          comment '订单id',
p_id                                                   int             comment '模板id',
interview_datetime                                     int             comment '面试时间',
is_one_ballot_veto                                     int             comment '是否一票否决 1 是 2 否',
is_accept_deploy                                       int             comment '是否接受全国调配 1 是 2 否',
is_pass                                                int             comment '是否通过 1 是 2 否',
interview_score_option                                 string          comment '面试得分选项数据',
interview_score_basic                                  int             comment '面试基础分',
interview_score_bonus                                  int             comment '面试加分得分',
interview_score                                        double          comment '面试总得分',
interview_evaluate                                     string          comment '面试综合评价',
second_interview_id                                    int             comment '二面面试官',
config_json_data                                       string          comment '模板类提交数据',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/zp_person_second_interview_result'
;
