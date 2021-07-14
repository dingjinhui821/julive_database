drop table if exists ods.yw_report;
create external table ods.yw_report(
id                                                     int             comment '',
order_id                                               bigint          comment '订单id',
project_id                                             bigint          comment '楼盘id',
employee_id                                            bigint          comment '报备咨询师id',
order_customer_id                                      int             comment '报备用户id',
order_customer_mobile                                  string          comment '报备用户手机号',
report_text                                            string          comment '报备信息（按模板表show_index排序的json格式）',
city_id                                                int             comment '城市id',
is_sign                                                int             comment '是否签到:1已签到，2未签到，3超时未签到',
sign_addr                                              string          comment '签到地点',
sign_coordinate                                        string          comment '签到坐标',
sign_datetime                                          int             comment '签到时间',
status                                                 int             comment '状态:1未确认，2待审核，3审核通过，4审核不通过',
note                                                   string          comment '备注',
visit_type                                             int             comment '到访方式:1初访，2复访',
visit_datetime                                         int             comment '到访时间',
reception_sale                                         string          comment '接待销售',
creator                                                bigint          comment '',
create_datetime                                        int             comment '',
updator                                                bigint          comment '',
update_datetime                                        int             comment '',
yw_consultant_track_id                                 bigint          comment 'yw_consultant_track_id 表id 用作脚本记录第一次打点信息',
audit_datetime                                         int             comment '审核时间',
report_employee_id                                     bigint          comment '报备人（确认报备记录的商务专员）',
report_employee_name                                   string          comment '报备人姓名',
checked_datetime                                       int             comment '报备确认时间',
log_id                                                 int             comment '最新审核日志id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_report'
;
