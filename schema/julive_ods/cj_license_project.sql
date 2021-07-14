drop table if exists ods.cj_license_project;
create external table ods.cj_license_project(
id                                                     bigint          comment '',
license_id                                             int             comment '许可证id',
project_id                                             int             comment '楼盘id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
business_construct_acreage                             double          comment '业态建筑面积',
business_constrict_num                                 int             comment '业态建筑套数',
business_sale_acreage                                  double          comment '业态外销面积',
business_sale_num                                      int             comment '业态外销套数',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_license_project'
;
