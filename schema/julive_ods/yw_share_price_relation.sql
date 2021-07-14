drop table if exists ods.yw_share_price_relation;
create external table ods.yw_share_price_relation(
id                                                     int             comment '主键id',
share_id                                               int             comment '分享id',
project_id                                             int             comment '楼盘id',
price_id                                               int             comment '周边id',
price_category                                         int             comment '分类(1地价,2:二手房价)',
price                                                  string          comment '价格',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
price_update_datetime                                  int             comment '价格更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_price_relation'
;
