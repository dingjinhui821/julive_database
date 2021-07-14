drop table if exists ods.bd_license_relation;
create external table ods.bd_license_relation(
id                                                     bigint          comment '',
license_id                                             int             comment '许可证id',
business_type                                          int             comment '类型 1楼盘 2证件楼栋',
business_id                                            int             comment '业务id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_license_relation'
;
