drop table if exists ods.zp_offer_follow;
create external table ods.zp_offer_follow(
id                                                     int             comment '',
order_id                                               bigint          comment '订单id',
follower_id                                            int             comment '跟进人id',
follow_time                                            int             comment '跟进时间',
is_offjob_certify                                      int             comment '是否有离职证明 1 有 2 没有',
is_diploma                                             int             comment '是否有学历证明 1 有 2没有',
is_health_certificate                                  int             comment '是否有健康证明 1 是 2 否',
is_herself_introduce                                   int             comment '是否有自我介绍 1 有 2 没有',
candidate_status                                       string          comment '候选人当前状态',
candidate_entry_intention                              string          comment '侯选人入职意向',
candidate_question                                     string          comment '候选人提问了哪些问题',
other_remark                                           string          comment '其他记录补充',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/zp_offer_follow'
;
