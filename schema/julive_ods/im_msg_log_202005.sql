drop table if exists ods.im_msg_log_202005;
create external table ods.im_msg_log_202005(
id                                                     int             comment '主键id',
from_account                                           string          comment '发送者',
to_account                                             string          comment '接收者',
sess_id                                                string          comment '会话id',
msg_timestamp                                          int             comment '消息发送时间',
msg_seq                                                int             comment '消息序列号',
msg_random                                             int             comment '消息随机数',
msg_body                                               string          comment '消息内容',
week                                                   int             comment '周',
day                                                    int             comment '天',
hour                                                   int             comment '时',
create_datetime                                        int             comment '创建时间',
update_dateitme                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/im_msg_log_202005'
;
