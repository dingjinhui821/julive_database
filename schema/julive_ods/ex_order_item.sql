drop table if exists ods.ex_order_item;
create external table ods.ex_order_item(
id                                                     int             comment '',
ex_order_id                                            int             comment 'bd单id',
business_type                                          int             comment '业务类型 1楼盘 2地块',
business_id                                            int             comment '业务id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_order_item'
;
