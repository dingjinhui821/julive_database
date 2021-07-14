drop table if exists ods.yw_quality_result;
create external table ods.yw_quality_result(
id                                                     int             comment 'id',
task_project_id                                        int             comment '作业id',
contact_call_id                                        int             comment '首电质检数据表id',
employee_id                                            int             comment '咨询师id',
quality_type                                           int             comment '质检类型1:普通质检2:a类问题3:无效样本4:中断录音',
b_score                                                int             comment 'b类得分',
no_score                                               int             comment '不计分数',
total_score                                            int             comment '总分',
pass_score                                             int             comment '及格分',
score                                                  int             comment '得分',
is_qualified                                           int             comment '是否合格1:合格2',
is_voice                                               int             comment '咨询师主管审批时是否听过录音（0:未听录音 1:已听录音）',
coach_advice                                           string          comment '辅导意见',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_quality_result'
;
