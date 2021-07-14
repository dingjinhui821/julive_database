drop table if exists ods.yw_see_project_list_commont;
create external table ods.yw_see_project_list_commont(
id                                                     bigint          comment '',
create_datetime                                        int             comment '创建时间',
creator                                                bigint          comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                bigint          comment '更新人',
order_id                                               bigint          comment '订单id',
see_project_id                                         bigint          comment '带看表id',
project_id                                             bigint          comment '楼盘id',
project_name                                           string          comment '楼盘名称',
project_comment                                        string          comment '楼盘评论',
quality_rate                                           int             comment '品质评星',
area_rate                                              int             comment '区域评星',
cost_rate                                              int             comment '性价比评星',
useless                                                int             comment '评价无用',
useful                                                 int             comment '评价有用',
project_grade                                          int             comment '楼盘评分',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_see_project_list_commont'
;
