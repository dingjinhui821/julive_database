drop table if exists julive_bak.yw_operation_log_copy_clean;
create external table julive_bak.yw_operation_log_copy_clean(
id                                                     bigint          comment 'id',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
employee_id                                            bigint          comment '员工id',
employee_name                                          string          comment '员工姓名',
user_ip                                                string          comment '用户ip',
power_en                                               string          comment '动作',
power_name                                             string          comment '动作名称',
way                                                    string          comment '方法 post或者get',
way_data                                               string          comment '方法数据 json',
url                                                    string          comment '请求的url',
referrer                                               string          comment '上次请求url',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/julive_bak/yw_operation_log_copy_clean'
;
