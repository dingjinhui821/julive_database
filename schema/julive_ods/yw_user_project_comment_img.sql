drop table if exists ods.yw_user_project_comment_img;
create external table ods.yw_user_project_comment_img(
id                                                     int             comment '',
create_datetime                                        int             comment '创建时间',
creator                                                bigint          comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                bigint          comment '更新人',
comment_id                                             bigint          comment '点评id',
img_url                                                string          comment '图片地址',
project_id                                             bigint          comment 'comjia新生成楼盘id',
status                                                 int             comment '状态  1待审核 2审核通过 3审核拒绝 4删除',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_user_project_comment_img'
;
