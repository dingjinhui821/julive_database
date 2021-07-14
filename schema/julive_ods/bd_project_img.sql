drop table if exists ods.bd_project_img;
create external table ods.bd_project_img(
id                                                     int             comment '',
newcode                                                bigint          comment 'newcode',
title                                                  string          comment '标题',
url                                                    string          comment '图片源地址',
registdate                                             int             comment '添加时间',
cid                                                    int             comment '分类id',
img_url                                                string          comment '图片本地地址',
project_id                                             bigint          comment '楼盘id',
source                                                 int             comment '来源',
auditor                                                bigint          comment '审核者',
audit_status                                           int             comment '审核状态:1未审核，2审核通过，3审核驳回',
audit_datetime                                         int             comment '审核时间',
img_type                                               int             comment '图片类型1样板间 2楼盘实景图 3配套实景图 4环境规划图 5效果图 6板块图 7详情页轮播图 8首页图片',
img_w                                                  int             comment '图片宽度(-1标识无法获取宽高)',
img_h                                                  int             comment '图片高度',
download_datetime                                      int             comment '图片下载时间',
state                                                  int             comment '0:未处理1:已处理2:非法url3:下载失败4:上传失败5:宽高获取失败',
watermark                                              string          comment '',
creator                                                bigint          comment '创建者',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '修改者',
update_datetime                                        int             comment '修改时间',
parent_img_id                                          int             comment '父楼盘图id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_project_img'
;
