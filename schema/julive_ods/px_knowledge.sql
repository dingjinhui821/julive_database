drop table if exists ods.px_knowledge;
create external table ods.px_knowledge(
id                                                     int             comment '',
order_id                                               int             comment '订单id',
score_content                                          string          comment '分数字符串',
knowledge_score                                        double          comment '通关考试分数',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/px_knowledge'
;
