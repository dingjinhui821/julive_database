drop table if exists ods.yw_report_error_record;
create external table ods.yw_report_error_record(
id                                                     bigint          comment '',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
err_level                                              int             comment '错误等级:1a级，2b级，3c级',
err_type                                               int             comment '错误类型:1报备，2带看，3反馈',
err_reason                                             int             comment '错误原因',
err_desc                                               string          comment '错误描述',
report_id                                              bigint          comment '报备id',
type                                                   int             comment '类型:1未报备错误，2报备错误',
city_id                                                int             comment '城市id',
employee_id                                            bigint          comment '报备咨询师id',
project_id                                             bigint          comment '项目id',
user_name                                              string          comment '客户姓名',
user_mobile                                            string          comment '客户电话',
visit_datetime                                         int             comment '到访时间',
detail                                                 string          comment '详情',
status                                                 int             comment '状态:1正常，2删除',
responsible_employee_id                                bigint          comment '责任咨询师',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_report_error_record'
;
