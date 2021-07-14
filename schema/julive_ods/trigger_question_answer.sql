drop table if exists ods.trigger_question_answer;
create external table ods.trigger_question_answer(
id                                                     bigint          comment '',
question_id                                            bigint          comment '问题id',
city_id                                                int             comment '城市id',
order_id                                               bigint          comment '订单id',
question_create_datetime                               int             comment '创建时间',
question_type                                          string          comment '问题类型id，逗号分隔',
follow_type                                            int             comment '跟进类型，1联系，2带看',
question_title                                         string          comment '问题标题',
question_status                                        int             comment '状态，1正常，2关闭',
questioner_type                                        int             comment '提问人类型，1用户，2咨询师',
questioner_id                                          bigint          comment '提问人id',
question_input_project                                 string          comment '问题录入楼盘',
question_interest_project                              string          comment '问题感兴趣楼盘',
question_see_project                                   string          comment '问题带看楼盘',
personal                                               int             comment '用户本人可见',
frontend                                               int             comment '网站展示',
backend                                                int             comment '咨询师内部',
answer_id                                              bigint          comment '答案id',
answer_create_datetime                                 int             comment '创建时间',
answer_content                                         string          comment '回答内容',
answer_img_count                                       int             comment '图片张数',
answer_like_num                                        int             comment '点赞数量',
answerer_type                                          int             comment '回答者类型，1用户，2咨询师',
answerer_id                                            bigint          comment '回答者id',
answer_status                                          int             comment '状态，1正常，2关闭',
answerer_is_show                                       int             comment '回答者是否展示，1为展示，2不展示，目前是通过触发器同步咨询师的状态',
is_attachment                                          int             comment '是否有附件 1有 2无',
score                                                  int             comment '答案得分（分值越大，得分越高）',
click_num                                              int             comment '问题点击量',
is_import_headline                                     int             comment '是否导入楼市头条（1:是,0:否）',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/trigger_question_answer'
;
