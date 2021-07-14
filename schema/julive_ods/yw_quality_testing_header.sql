drop table if exists ods.yw_quality_testing_header;
create external table ods.yw_quality_testing_header(
id                                                     int             comment '',
create_datetime                                        int             comment '创建时间',
creator                                                bigint          comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                bigint          comment '更新人',
order_id                                               bigint          comment '订单id',
city_id                                                bigint          comment '城市id',
city_name                                              string          comment '城市名称',
user_id                                                bigint          comment '用户id',
user_mobile                                            string          comment '用户电话',
distribute_datetime                                    int             comment '分配时间',
employee_id                                            bigint          comment '所属咨询师id',
employee_name                                          string          comment '所属咨询师',
employee_leader_id                                     bigint          comment '所属咨询师主管id',
employee_leader_name                                   string          comment '所属咨询师主管',
first_contact_close_type                               string          comment '首电关闭类型分类',
first_contact_quality_result                           string          comment '咨询师首电质检结果',
first_contact_quality_score                            int             comment '咨询师首电质检分数',
follow_see_quality_result                              string          comment '咨询师跟看质检结果',
follow_see_quality_score                               int             comment '咨询师跟看质检分数',
service_note                                           string          comment '服满备注',
quality_employee_id                                    bigint          comment '质检人id',
quality_employee                                       string          comment '质检人',
quality_datetime                                       int             comment '质检时间',
re_quality_result                                      string          comment '复检结果',
re_quality_employee_id                                 bigint          comment '复检人id',
re_quality_employee                                    string          comment '复检人',
re_quality_datetime                                    int             comment '复检时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_quality_testing_header'
;
