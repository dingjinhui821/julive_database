drop table if exists ods.kfs_order_attr;
create external table ods.kfs_order_attr(
id                                                     int             comment '主键id',
order_id                                               int             comment '订单id',
order_remark                                           string          comment '订单详情，订单备注',
intent_low_datetime                                    int             comment '无意向时间',
copy_order                                             int             comment '是否存在活跃订单 0:否 1:是',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
creator                                                int             comment '创建人',
updator                                                int             comment '修改人',
comjia_platform_id                                     int             comment '平台id',
product_id                                             int             comment '项目id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_order_attr'
;
