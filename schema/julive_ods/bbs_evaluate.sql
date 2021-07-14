drop table if exists ods.bbs_evaluate;
create external table ods.bbs_evaluate(
id                                                     int             comment '',
forum_id                                               int             comment '帖子id',
employee_id                                            int             comment '用户id',
evaluate_datetime                                      int             comment '评价时间',
result_score                                           int             comment '处理结果评分',
efficiency_score                                       int             comment '处理时效评分',
other_content                                          string          comment '其他内容',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bbs_evaluate'
;
