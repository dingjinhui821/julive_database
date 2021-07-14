drop table if exists ods.yw_quality_detail;
create external table ods.yw_quality_detail(
id                                                     int             comment 'id',
contact_call_id                                        int             comment '首电质检数据表id',
question_type                                          int             comment '问题类型 1.基本礼仪 2.需求挖掘 3.楼盘推荐 4.品牌建设 5.邀约情况 6.b类服务禁止项',
question_id                                            int             comment '问题id',
answer_id                                              int             comment '答案id',
answer_desc                                            string          comment '子类答案',
score                                                  int             comment '得分',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_quality_detail'
;
