drop table if exists ods.kfs_developer_project_relation;
create external table ods.kfs_developer_project_relation(
id                                                     int             comment '主键id',
developer_id                                           int             comment '开发商id',
project_id                                             int             comment '楼盘id',
city_id                                                int             comment '城市id',
project_mobile                                         string          comment '楼盘电话',
is_delete                                              int             comment '1未删除 2.已删除',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
is_purchase_flow                                       int             comment '是否购买流量 1是，2否',
is_up                                                  int             comment '是否置顶，记录时间戳',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_developer_project_relation'
;
