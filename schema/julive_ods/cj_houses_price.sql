drop table if exists ods.cj_houses_price;
create external table ods.cj_houses_price(
houses_price_id                                        bigint          comment '户型价格id',
project_id                                             bigint          comment '楼盘id',
admin_id                                               int             comment '发布人id',
submit_time                                            int             comment '发布时间',
sell_month                                             int             comment '销售月份',
price                                                  double          comment '当月价格',
bargain                                                int             comment '当月成交量',
surplus                                                int             comment '当月余量',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_houses_price'
;
