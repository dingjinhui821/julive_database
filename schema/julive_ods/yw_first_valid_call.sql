drop table if exists ods.yw_first_valid_call;
create external table ods.yw_first_valid_call(
id                                                     int             comment '',
total_time                                             int             comment '总时长',
number                                                 string          comment '语音个数',
start_call_time                                        int             comment '开始通话时间',
status                                                 int             comment '质检状态:1取消联系2质检中3质检完成4质检结果确认',
fraction                                               int             comment '分数',
manual_update_datetime                                 int             comment '修改更新时间',
order_id                                               bigint          comment '订单id',
business_id                                            bigint          comment '业务id',
business_type                                          int             comment '业务类型:1联系，2带看，3排号，4认购，5草签，6网签，7回访',
city_id                                                int             comment '城市id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_first_valid_call'
;
