drop table if exists ods.hj_experiment_group;
create external table ods.hj_experiment_group(
id                                                     int             comment '主键id',
main_id                                                int             comment '实验主表id',
group_id                                               string          comment '全局唯一标识符',
reason                                                 string          comment '原因说明',
group_type                                             int             comment '分组类型1.对照组,2实验组',
group_description                                      string          comment '分组描述',
cover_rate                                             double          comment '操作覆盖占比',
old_name                                               string          comment '文件老名称',
file_url                                               string          comment '文件地址',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hj_experiment_group'
;
