drop table if exists ods.yw_rate;
create external table ods.yw_rate(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
user_id                                                bigint          comment '用户id',
user_name                                              string          comment '用户名称',
project_id                                             bigint          comment '楼盘id',
project_name                                           string          comment '楼盘名称',
quality_rate                                           int             comment '品质评星',
area_rate                                              int             comment '区域评星',
cost_rate                                              int             comment '性价比评星',
content                                                string          comment '评价内容',
useless                                                int             comment '评价无用',
useful                                                 int             comment '评价有用',
status                                                 int             comment '1:假删除 2:待审核, 3有效',
type                                                   int             comment '评论的类型，1为带看用户对带看楼盘的评论 2为网站前台用户对楼盘的评论',
common_id                                              bigint          comment '对应type字段，如(带看用户对带看楼盘的评论)表yw_see_project_list_commont表的id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_rate'
;
