drop table if exists ods.bd_project_match;
create external table ods.bd_project_match(
id                                                     bigint          comment '',
bd_project_id                                          bigint          comment '楼盘id',
group_project_id                                       bigint          comment '聚合id',
project_name                                           string          comment '',
match_type                                             int             comment '匹配类型 1:精确匹配 2:模糊匹配 3:人工匹配',
state                                                  int             comment '审核状态 1:未审核  2:待定  3:已审核',
is_delete                                              int             comment '是否删除 1:未删除 2:已删除',
create_datetime                                        int             comment '添加时间',
source                                                 int             comment '源:1搜房2安居客3搜狐4腾讯99侃家',
update_datetime                                        int             comment '更新时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_project_match'
;
