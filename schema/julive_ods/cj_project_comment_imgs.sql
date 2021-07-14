drop table if exists ods.cj_project_comment_imgs;
create external table ods.cj_project_comment_imgs(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
relax_id                                               bigint          comment '图片对应评论表id',
uper_id                                                bigint          comment '上传者id 可能是用户id亦或咨询师id',
project_id                                             bigint          comment '楼盘id',
img_path                                               string          comment '图片',
show_index                                             int             comment '展示排序',
type                                                   int             comment '评论图片的类型，1为咨询师对楼盘的评论对应的图片 2为网站前台用户对楼盘的评论对应的图片 ,3为咨询师工作随笔的评论的图片',
status                                                 int             comment '1:假删除 2:待审核 3:有效',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_comment_imgs'
;
