drop table if exists ods.cj_license_building;
create external table ods.cj_license_building(
id                                                     bigint          comment '',
license_id                                             int             comment '许可证id',
building_name                                          string          comment '楼栋名称',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_license_building'
;
