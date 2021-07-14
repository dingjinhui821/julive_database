drop table if exists ods.yw_user_profile_feedback;
create external table ods.yw_user_profile_feedback(
id                                                     int             comment '主键id',
order_id                                               int             comment '订单id',
employee_id                                            int             comment '员工id',
post_id                                                int             comment '岗位id',
help                                                   int             comment '1有帮助 2没帮助',
trace_id                                               string          comment '大数据traceid',
attention                                              string          comment '关注项|分隔',
suggestion                                             string          comment '建议',
draft_content                                          string          comment '草稿内容',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_user_profile_feedback'
;
