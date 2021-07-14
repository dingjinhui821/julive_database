drop table if exists ods.yw_order_audio;
create external table ods.yw_order_audio(
id                                                     int             comment '主键id',
order_id                                               int             comment '订单id',
unique_id                                              string          comment '呼叫id:呼入和呼出',
type                                                   string          comment '呼叫类型:1呼入，2呼出',
audio_path                                             string          comment '语音远程地址',
audio_time_long                                        string          comment '语音时长',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
customer_number                                        string          comment '客户电话',
employee_name                                          string          comment '记录听过录音的咨询师',
city_id                                                bigint          comment '城市',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_audio'
;
