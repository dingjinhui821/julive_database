drop table if exists ods.yw_nim_msg;
create external table ods.yw_nim_msg(
id                                                     bigint          comment '',
eventtype                                              int             comment '事件类型，1会话',
convtype                                               string          comment '会话具体类型',
`to`                                                   string          comment '',
fromaccount                                            string          comment '消息发送者的用户账号',
fromclienttype                                         string          comment '发送客户端类型',
fromdeviceid                                           string          comment '发送设备id',
fromnick                                               string          comment '发送方昵称',
msgtimestamp                                           bigint          comment '消息发送时间',
msgtype                                                string          comment '会话具体类型person、team对应的通知消息类型',
body                                                   string          comment '消息内容',
attach                                                 string          comment '附加消息',
msgidclient                                            string          comment '客户端生成的消息id，仅在convtype为person或team含此字段',
msgidserver                                            string          comment '服务端生成的消息id',
resendflag                                             int             comment '0不是重发, 1是重发',
customsafeflag                                         int             comment '自定义系统通知消息是否存离线:0:不存，1:存',
customapnstext                                         string          comment '自定义系统通知消息推送文本。仅在convtype为custom_person或custom_team时含此字段',
tmembers                                               string          comment '跟本次群操作有关的用户accid，仅在convtype为team或custom_team时含此字段',
ext                                                    string          comment '跟本次群操作有关的用户accid，仅在convtype为team或custom_team时含此字段',
antispam                                               string          comment '消息扩展字段',
create_datetime                                        int             comment '创建时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_nim_msg'
;
