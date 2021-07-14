drop table if exists ods.kfs_project_follow_flow;
create external table ods.kfs_project_follow_flow(
id                                                     int             comment '主键id',
follow_id                                              int             comment '跟进主表id,对应kfs_project_follow表中id',
estimated_payment_money                                int             comment '预计付费金额',
estimated_arrival_time                                 int             comment '预计到账时间',
estimated_delivery_time                                int             comment '预计投放时间',
avg_clue_num                                           int             comment '期望日均线索量',
avg_clue_money                                         int             comment '期望日均线索成本',
high_clue_num                                          int             comment '累计高意向线索量',
total_see_num                                          int             comment '累计带看量',
total_subscribe_num                                    int             comment '累计认购量',
remark                                                 string          comment '备注',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_project_follow_flow'
;
