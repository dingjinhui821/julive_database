drop table if exists ods.yw_project_review;
create external table ods.yw_project_review(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
creator                                                bigint          comment '创建人',
updator                                                bigint          comment '更新人',
project_id                                             bigint          comment '楼盘id',
project_name                                           string          comment '楼盘名字',
review                                                 string          comment '咨询师对楼盘的点评',
reject_reason                                          string          comment '驳回原因',
employee_id                                            bigint          comment '',
employee_name                                          string          comment '',
status                                                 int             comment '点评的状态，0为审核失败，1为审核中，2为通过审核',
area_id                                                bigint          comment '区域id',
area_name                                              string          comment '区域名字',
like_base                                              int             comment '点赞基数',
like_show                                              int             comment '点赞显示',
review_length                                          int             comment '点评字数',
review_is_show                                         int             comment '点评是否显示:1显示 2不显示',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
audit_time                                             int             comment '审核时间（通过或驳回时间）',
operator                                               int             comment '审批通过/驳回人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_project_review'
;
