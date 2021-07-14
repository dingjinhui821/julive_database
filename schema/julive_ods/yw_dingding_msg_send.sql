drop table if exists ods.yw_dingding_msg_send;
create external table ods.yw_dingding_msg_send(
id                                                     bigint          comment 'id',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
send_to                                                string          comment '钉钉消息接受者',
status                                                 int             comment '发送状态，0为无效，1为未发送，2为已发送',
content                                                string          comment '信息内容',
send_num                                               int             comment '消息发送次数',
task_id                                                bigint          comment '任务id',
messageurl                                             string          comment '指定要跳转的url',
title                                                  string          comment '标题',
msgtype                                                int             comment '消息类型1:文本2:链接3:机器人markdown消息',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_dingding_msg_send'
;
