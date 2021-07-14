drop table if exists ods.kfs_developer_info;
create external table ods.kfs_developer_info(
id                                                     int             comment '主键id',
developer_name                                         string          comment '开发商名称',
developer_logo                                         string          comment '开发商logo',
developer_desc                                         string          comment '开发商备注',
is_delete                                              int             comment '是否删除（1-未删 2-已删）',
project_num                                            int             comment '合作楼盘数量',
city_num                                               int             comment '合作城市数量',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_developer_info'
;
