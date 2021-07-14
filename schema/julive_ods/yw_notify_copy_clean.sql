drop table if exists julive_bak.yw_notify_copy_clean;
create external table julive_bak.yw_notify_copy_clean(
id                                                     int             comment '主键自增长',
send_id                                                bigint          comment '发送者id',
send_name                                              string          comment '发送者姓名',
title                                                  string          comment '标题',
contents                                               string          comment '内容',
url                                                    string          comment '跳转链接',
create_datetime                                        int             comment '创建时间',
remind_datetime                                        int             comment '提醒时间',
receive_id                                             bigint          comment '接收人id',
receive_man                                            string          comment '接收人',
status                                                 int             comment '消息状态 0:未发送 1:已发送 -1已删除',
update_datetime                                        bigint          comment '更新时间',
msg_type                                               int             comment '0未知类型的消息（普通消息），1新用户首次分配（待办事项），2首次联系用户（待办事项），3预约联系到时提醒（普通消息），4预约带看到时提醒（普通消息），5申请楼盘报备（待办事项），6更新报备状态（普通消息），7邀请别人帮带看（待办事项），8确认接受别人的邀请带看（普通消息），9确认收到新用户分配（普通消息）',
handle_id                                              bigint          comment '对应的业务id',
async_random                                           int             comment '执行随机数，默认值1，系统会按照此随机数来分成多个脚本执行',
real_contents                                          string          comment '包含真实手机号内容',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/julive_bak/yw_notify_copy_clean'
;
