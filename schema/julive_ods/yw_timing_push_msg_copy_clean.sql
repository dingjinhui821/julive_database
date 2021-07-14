drop table if exists julive_bak.yw_timing_push_msg_copy_clean;
create external table julive_bak.yw_timing_push_msg_copy_clean(
id                                                     int             comment 'id',
msg_id                                                 string          comment '消息id',
msg_type                                               int             comment '消息类型',
product_type                                           int             comment '1:app_e,2:app_u',
receive_conf                                           string          comment '推送用户配置',
sms_type                                               int             comment '短信类型',
sms_params                                             string          comment '短信参数',
notification                                           string          comment '推送消息内容',
push_params                                            string          comment '推送参数',
send_time_conf                                         string          comment '推送时间配置',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
is_send                                                int             comment '短信是否发送，0未发送，1已发送',
is_push                                                int             comment '消息是否推送，0未推送，1已推送',
push_return                                            string          comment '推送返回结果',
push_status                                            int             comment '推送状态，1成功，2失败',
send_time                                              int             comment '期望发送的时间',
send_check_sql                                         string          comment '到达发送时间时检验是否真的需要发送',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/julive_bak/yw_timing_push_msg_copy_clean'
;
