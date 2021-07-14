drop table if exists ods.qa_answer;
create external table ods.qa_answer(
id                                                     bigint          comment '',
creator                                                bigint          comment '创建人id',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '更新人',
update_datetime                                        int             comment '更新人',
parent_id                                              bigint          comment '父id',
question_id                                            bigint          comment '问题id',
answerer_type                                          int             comment '回答者类型，1用户，2咨询师',
answerer_id                                            bigint          comment '回答者id',
answerer_nickname                                      string          comment '回答者昵称',
content                                                string          comment '回答内容',
attachment                                             string          comment '答案附件，json存储',
status                                                 int             comment '状态，1正常，2删除',
img_count                                              int             comment '图片张数',
imgs_info                                              string          comment '图片信息',
like_num                                               int             comment '点赞数量',
score                                                  int             comment '得分（分值越大，得分越高）',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/qa_answer'
;
