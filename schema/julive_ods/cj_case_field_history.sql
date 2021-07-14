drop table if exists ods.cj_case_field_history;
create external table ods.cj_case_field_history(
id                                                     bigint          comment '',
project_id                                             int             comment '楼盘id',
case_field_id                                          int             comment '案场id',
statistics_datetime                                    int             comment '统计时间',
visit_num                                              int             comment '到访总量',
paihao_num                                             int             comment '排号总量',
subscribe_num                                          int             comment '认购总量',
note                                                   string          comment '案场数据补充说明',
channel_json                                           string          comment '渠道信息json',
is_average                                             int             comment '是否是平均值 1是 2否',
average_start_datetime                                 int             comment '平均值开始时间',
average_end_datetime                                   int             comment '平均值结束时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_case_field_history'
;
