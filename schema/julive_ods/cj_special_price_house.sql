drop table if exists ods.cj_special_price_house;
create external table ods.cj_special_price_house(
id                                                     int             comment '',
house_num                                              string          comment '房号',
acreage                                                double          comment '面积',
price                                                  double          comment '原价(原始数据)',
special_price                                          double          comment '特价(原始数据)',
summary                                                string          comment '其他说明',
start_datetime                                         int             comment '特价开始时间',
end_datetime                                           int             comment '特价结束时间',
project_id                                             bigint          comment '楼盘id',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
is_show                                                int             comment '是否显示 1显示 2隐藏',
end_datetime_specific                                  int             comment '指定结束时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_special_price_house'
;
