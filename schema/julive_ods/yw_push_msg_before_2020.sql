drop table if exists julive_bak.yw_push_msg_before_2020;
create external table julive_bak.yw_push_msg_before_2020(
id                                                     bigint          comment '自增id',
julive_msg_id                                          string          comment '居理生成的msg_id,客端去重用',
msg_type                                               int             comment '消息类型',
msg_id                                                 string          comment '推送时厂商返回的消息id',
reg_ids                                                string          comment '接受人{“unique_id”=>reg_id,unique_id=>}最多100人',
app_id                                                 int             comment '居理的app_id',
batch                                                  string          comment '推送批次',
title                                                  string          comment '标题',
notification                                           string          comment '要发送的内容',
is_push                                                int             comment '消息是否推送，0未推送，1已推送',
send_time                                              int             comment '期望发送时间',
preference                                             int             comment '1:优先发送,值越小,越先发送',
push_return                                            string          comment '推送的结果',
push_status                                            int             comment '推送状态，1成功，2失败',
push_params                                            string          comment '推送参数',
type                                                   int             comment '类型1:小米,2:华为,3:极光',
send_times                                             int             comment '发送次数',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
push_data                                              string          comment '提交给第三方的数据',
push_count                                             int             comment '推送的设备数量',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/julive_bak/yw_push_msg_before_2020'
;
