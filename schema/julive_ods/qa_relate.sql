drop table if exists ods.qa_relate;
create external table ods.qa_relate(
id                                                     int             comment '',
relate_type                                            int             comment '关联类型 1问题 2答案',
object_type                                            int             comment '对象类型，1录入楼盘，2感兴趣楼盘，3带看楼盘',
object_id                                              bigint          comment '对象id',
question_id                                            bigint          comment '问题id',
answer_id                                              bigint          comment '答案id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
correlation                                            int             comment '相关度',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/qa_relate'
;
