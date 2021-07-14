drop table if exists ods.cj_project_img_history;
create external table ods.cj_project_img_history(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
project_id                                             bigint          comment '楼盘id',
type                                                   int             comment '图片分类，取自常量配置表，group_name:project_img, 1样板间 2楼盘实景图 3配套实景图 4环境规划图 5效果图 6板块图 7详情页轮播图 8首页图片',
img_url                                                string          comment '图片地址',
show_index                                             int             comment '排序',
status                                                 int             comment '状态，1:有效  2:假删除',
title                                                  string          comment '标题',
project_img_id                                         bigint          comment '',
old_source_id                                          int             comment '旧数据对应资源id',
`desc`                                                 string          comment '图片描述，不超过50个字',
auditor                                                bigint          comment '审核者',
audit_status                                           int             comment '审核状态:1未审核，2审核通过，3审核驳回',
audit_datetime                                         int             comment '审核时间',
is_home_page_img                                       int             comment '是否为首页图，0否，1是',
is_home_page_banner                                    int             comment '是否是首页轮换图0:否 1:是',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_img_history'
;
