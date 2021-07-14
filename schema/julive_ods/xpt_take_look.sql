drop table if exists ods.xpt_take_look;
create external table ods.xpt_take_look(
id                                                     int             comment '主键id',
report_id                                              int             comment '报备id',
order_id                                               int             comment '订单id',
appointment_time                                       int             comment '预约带看时间',
realy_time                                             int             comment '实际带看时间',
is_valid                                               int             comment '是否有效 0未判定 1有效 2无效',
status                                                 int             comment '带看状态 1预约带看 2实际带看',
counselor_name                                         string          comment '置业顾问姓名',
counselor_mobile                                       string          comment '置业顾问手机号',
invalid_reason                                         string          comment '无效原因',
cancle_reason                                          string          comment '取消原因',
store_id                                               int             comment '门店id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建时间',
updator                                                int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_take_look'
;
