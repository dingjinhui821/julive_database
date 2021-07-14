drop table if exists julive_bak.yw_message_copy_clean;
create external table julive_bak.yw_message_copy_clean(
id                                                     bigint          comment '主键自增长',
to_user_id                                             bigint          comment '接收人的id',
content                                                string          comment '提醒内容',
msg_type                                               int             comment '0未知类型的消息（普通消息），1新用户首次分配（待办事项），2首次联系用户（待办事项），3预约联系到时提醒（普通消息），4预约带看到时提醒（普通消息），5申请楼盘报备（待办事项），6更新报备状态（普通消息），7邀请别人帮带看（待办事项），8确认接受别人的邀请带看（普通消息），9确认收到新用户分配（普通消息）',
from_name                                              string          comment '发送人的名字',
need_reply                                             int             comment '消息回执(0不回执,1回执)',
title                                                  string          comment '提醒标题',
url                                                    string          comment '链接',
status                                                 int             comment '状态(1:未读,2:已读，-1已删除（软删除会留底）,0表示不需要关注已读未读),-2表示彻底删除',
update_datetime                                        int             comment '更新时间',
create_datetime                                        int             comment '创建时间',
todo_check_sql                                         string          comment '检测待办事项是否已经完成的查询sql，如果查询有结果表示待办事项已完成',
real_content                                           string          comment '包含真实手机号内容',
handle_id                                              int             comment '业务id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/julive_bak/yw_message_copy_clean'
;
