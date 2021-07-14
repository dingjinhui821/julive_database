drop table if exists ods.ex_project_is_main_push;
create external table ods.ex_project_is_main_push(
id                                                     int             comment '',
project_id                                             int             comment '楼盘id',
see_project_target                                     int             comment '带看目标',
subscribe_target                                       int             comment '认购(排卡)目标',
incostomer_target                                      int             comment '上户目标',
project_type                                           int             comment '业态',
min_budget                                             double          comment '最小预算',
max_budget                                             double          comment '最大预算',
push_reason_type                                       int             comment '主推原因类型(配置里面取)',
status                                                 int             comment '是否主推(1,作为主推 2不作为主推 3取消主推)',
cancel_push_reason                                     string          comment '取消主推原因',
not_push_reason_type                                   int             comment '不主推原因(配置里面取)',
week_start                                             int             comment '周开始时间',
city_id                                                int             comment '城市id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_project_is_main_push'
;
