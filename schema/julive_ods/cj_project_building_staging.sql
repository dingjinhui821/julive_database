drop table if exists ods.cj_project_building_staging;
create external table ods.cj_project_building_staging(
id                                                     bigint          comment '',
project_id                                             int             comment '楼盘id',
building_id                                            int             comment '楼栋id',
staging_name                                           string          comment '分期名称',
staging_sale_status                                    int             comment '分期销售状态 2在售 ,3售罄,4待售',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_building_staging'
;
