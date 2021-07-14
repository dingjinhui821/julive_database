drop table if exists ods.yw_quality_question;
create external table ods.yw_quality_question(
id                                                     int             comment 'id',
question_type                                          int             comment '问题类型 1.基本礼仪 2.需求挖掘 3.楼盘推荐 4.品牌建设 5.邀约情况 6.b类服务禁止项',
title                                                  string          comment '问题标题',
score_value                                            int             comment '分值',
question_desc                                          string          comment '问题描述',
parent_id                                              int             comment '父级id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_quality_question'
;
