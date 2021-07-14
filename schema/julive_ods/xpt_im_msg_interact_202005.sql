drop table if exists ods.xpt_im_msg_interact_202005;
create external table ods.xpt_im_msg_interact_202005(
id                                                     int             comment '主键id',
from_account                                           string          comment '发送者',
to_account                                             string          comment '接收者',
msg_key                                                string          comment 'im消息id',
is_first                                               int             comment '是否用户消息第一条',
is_minute_reply                                        int             comment '是否是一分钟内回复 1.是， 2.否',
replay_wait_time                                       int             comment '回复等待时长秒',
sess_id                                                string          comment '会话id',
send_time                                              int             comment '发送时间',
type                                                   int             comment '消息类型 2.员工to用户， 1.用户to员工',
is_read                                                int             comment '是否已读 1.是， 2.否',
is_reply                                               int             comment '是否回复 1.是， 2.否',
server_city                                            int             comment '服务城市',
year                                                   int             comment '年',
month                                                  int             comment '月',
week                                                   int             comment '周',
day                                                    int             comment '天',
hour                                                   int             comment '时',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_im_msg_interact_202005'
;
