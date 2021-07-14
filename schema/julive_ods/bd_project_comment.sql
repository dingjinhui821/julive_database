drop table if exists ods.bd_project_comment;
create external table ods.bd_project_comment(
id                                                     int             comment '',
username                                               string          comment '用户名',
content                                                string          comment '',
create_datetime                                        int             comment '创建时间',
comment_rat                                            int             comment '评分',
source                                                 int             comment '来源1搜房2安居客3搜狐焦点4腾讯看房',
project_id                                             bigint          comment '抓取过来的楼盘id',
images                                                 string          comment '评论图片',
comment_id                                             bigint          comment '源网站上的评论id',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '修改者',
update_datetime                                        int             comment '修改时间',
publish_datetime                                       int             comment '发布时间',
review                                                 int             comment '审核状态:0未审核1审核通过2不通过',
auditor                                                int             comment '审核者',
comment_num                                            int             comment '点赞数量',
yw_user_comment_id                                     int             comment 'comjia用户点评楼盘表id',
parent_comment_id                                      int             comment '父评论id',
is_repeat                                              int             comment '是否是重复点评 1是 2否',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_project_comment'
;
