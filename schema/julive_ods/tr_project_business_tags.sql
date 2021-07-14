drop table if exists ods.tr_project_business_tags;
create external table ods.tr_project_business_tags(
id                                                     bigint          comment '',
creator                                                int             comment '创建人',
create_datetime                                        int             comment '创建时间',
updator                                                int             comment '更新人',
update_datetime                                        int             comment '更新时间',
label_type                                             int             comment '标签类型',
label_value                                            int             comment '标签值',
label_name                                             string          comment '标签名称',
employee_id                                            bigint          comment '咨询师id',
is_cancel                                              int             comment '0 未申诉取消 1 已申诉取消',
business_datetime                                      int             comment '打标签业务时间',
cause                                                  int             comment '1.未发布学习任务 2.重复学习 3.考试未通过 4.没有上传学习成绩',
deduct_score                                           int             comment '扣除分数',
cancel_employee                                        int             comment '取消人',
cancel_datetime                                        int             comment '取消时间',
employee_leader_id                                     int             comment '咨询师主管',
city_id                                                int             comment '城市id',
cancel_type                                            int             comment '取消类型（1-系统取消、2-申诉取消、3-人工取消、4-脚本取消）',
cancel_console                                         string          comment '脚本取消、执行的脚本及方法名称',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/tr_project_business_tags'
;
