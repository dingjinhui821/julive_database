drop table if exists ods.yw_quality_testing_first_contact;
create external table ods.yw_quality_testing_first_contact(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
city_id                                                bigint          comment '城市id',
city_name                                              string          comment '城市名称',
user_mobile                                            string          comment '客户电话号码',
employee_id                                            bigint          comment '所属咨询师',
employee_name                                          string          comment '咨询师姓名',
employee_manage_id                                     bigint          comment '咨询师主管id',
employee_manage_name                                   string          comment '咨询师主管名称',
status                                                 int             comment '是否删除:-1已删除1未删除',
order_status                                           int             comment '订单状态:0:非正常结束（半路中断，或没买房）10:未确认收到分配 20:已收到分配 30:已联系 40:已带看 50:认购 60:已签约',
first_contact_quality_result                           string          comment '咨询师首电质检结果',
first_contact_quality_score                            int             comment '咨询师首电质检分数',
quality_employee                                       string          comment '质检人',
quality_employee_id                                    bigint          comment '质检人id',
quality_datetime                                       int             comment '质检时间',
tag_content                                            string          comment '标记类型',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
distribute_datetime                                    int             comment '分配时间',
job_number                                             string          comment '咨询师工号',
quality_result_id                                      int             comment '首电质检结果表id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_quality_testing_first_contact'
;
