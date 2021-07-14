drop table if exists ods.yw_order_contact;
create external table ods.yw_order_contact(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
order_id                                               bigint          comment '订单id',
employee_id                                            bigint          comment '员工id',
user_id                                                bigint          comment '用户id',
plan_real_datetime                                     int             comment '计划/实际 联系时间',
plan_content                                           string          comment '计划联系内容',
real_content                                           string          comment '实际联系内容',
history_employee_id                                    int             comment '历史咨询师',
contact_employee                                       int             comment '联系咨询师',
status                                                 int             comment '0:取消，1:已预约2，已实际联系',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
intent_low_desc                                        string          comment '关闭订单情况说明',
follow_tags                                            string          comment '跟进频率标签',
is_overtime                                            int             comment '是否超时0:未超时 1:已超时',
overtime_days                                          int             comment '超时天数',
contact_timeout_imgs                                   string          comment '首次跟进实际联系截图凭证',
plan_datetime                                          int             comment '计划联系时间',
plan_to_real_datetime                                  int             comment '计划转实际时间',
type                                                   int             comment '联系类型 1.取消最后一次预约产生的实际联系2.关闭订单产生的实际联系3.暂停订单',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_contact'
;
