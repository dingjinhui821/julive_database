drop table if exists ods.xpt_timing_wx_msg;
create external table ods.xpt_timing_wx_msg(
id                                                     int             comment '自增id',
template_id                                            string          comment '模板id',
user_type                                              int             comment '用户类型 1:c端用户 2:b端用户',
send_time                                              int             comment '发送时间',
openid                                                 string          comment '消息接收人openid',
content                                                string          comment '发送内容',
is_send                                                int             comment '是否发送1:未发送,2:已发送,-1:已经删除',
send_return                                            string          comment '发送结果,第三方返回的信息',
send_status                                            int             comment '微信返回的发送结果 0:发送成功',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_timing_wx_msg'
;
