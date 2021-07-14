drop table if exists ods.yw_qa_quality_testing;
create external table ods.yw_qa_quality_testing(
id                                                     bigint          comment '',
question_id                                            bigint          comment '问题id',
order_id                                               bigint          comment '订单id',
city_id                                                int             comment '城市id',
employee_id                                            bigint          comment '员工id',
qa_create_datetime                                     int             comment '问答创建时间',
project_name                                           string          comment '录入楼盘名称',
project_id                                             string          comment '录入楼盘id',
is_frontend                                            int             comment '是否前端展示:1是0否',
is_pass                                                int             comment '是否及格:0否 1是',
cause_failure                                          string          comment '不及格原因',
upload_time                                            int             comment '上传时间',
qa_type                                                string          comment '问题类型id，逗号分隔',
is_delete                                              int             comment '是否删除:0否 1是',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_qa_quality_testing'
;
