drop table if exists ods.cj_project_volume_relation;
create external table ods.cj_project_volume_relation(
id                                                     bigint          comment '',
volume_id                                              int             comment '放量计划id',
business_type                                          int             comment '业务类型 1户型 2楼栋',
business_id                                            int             comment '业务id',
volume_num                                             int             comment '放量数量',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_volume_relation'
;
