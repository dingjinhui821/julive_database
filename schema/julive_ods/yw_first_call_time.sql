drop table if exists ods.yw_first_call_time;
create external table ods.yw_first_call_time(
id                                                     int             comment '',
first_call_time_long                                   double          comment '首次回拨时长',
first_call_valid_time_long                             int             comment '首次回拨有效时长',
order_id                                               int             comment '订单id',
mobile                                                 string          comment '客户手机号--字段废弃',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
creator                                                int             comment '',
updator                                                int             comment '',
follow_service                                         int             comment '客服',
current_datetime                                       int             comment '时间',
num                                                    int             comment '接单数量',
ftime                                                  double          comment '累计开接单时长',
order_time                                             double          comment '当前开关接单时长',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_first_call_time'
;
