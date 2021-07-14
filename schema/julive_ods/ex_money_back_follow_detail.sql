drop table if exists ods.ex_money_back_follow_detail;
create external table ods.ex_money_back_follow_detail(
id                                                     int             comment 'id',
group_id                                               int             comment '回款组id',
follow_id                                              int             comment '跟进id',
follow_stage_type                                      int             comment '跟进阶段',
follow_result                                          int             comment '跟进结果',
file_type                                              int             comment '凭证类型 1. 标准:业绩确认单 2.非标准:微信聊天等',
business_datetime                                      int             comment '业务时间',
follow_reason                                          string          comment '跟进原因',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_money_back_follow_detail'
;
