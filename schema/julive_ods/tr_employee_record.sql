drop table if exists ods.tr_employee_record;
create external table ods.tr_employee_record(
id                                                     bigint          comment '',
current_task_id                                        int             comment '任务id',
creator                                                int             comment '',
updator                                                int             comment '',
project_id                                             int             comment '咨询师学习的楼盘id',
employee_id                                            int             comment '员工id',
whether_mastered                                       int             comment '是否掌握   0默认 1已掌握 2未掌握',
score                                                  double          comment '平均分',
exam_score                                             double          comment '考试成绩',
speak_score                                            double          comment '讲盘成绩',
re_exam_score                                          double          comment '补考考试成绩',
re_speak_score                                         double          comment '补考讲盘成绩',
department_id                                          int             comment '部门id',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
employee_manager_id                                    int             comment '咨询主管id',
employee_manager_name                                  string          comment '咨询师主管名称',
exam_standard                                          string          comment '考试标准',
is_pass                                                int             comment '是否通过考试（0未录入，1通过，2未通过,3录入中）',
city_id                                                int             comment '',
pass_type                                              int             comment '通过类型（上传通过=1，补考通过=2）',
project_pass_time                                      int             comment '楼盘通过时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/tr_employee_record'
;
