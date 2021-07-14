drop table if exists ods.kfs_project_follow_flow_detail;
create external table ods.kfs_project_follow_flow_detail(
id                                                     int             comment '主键id',
follow_id                                              int             comment '跟进主表id,对应kfs_project_follow表中id',
detail_datetime                                        int             comment '明细日期',
clue_target                                            int             comment '线索目标',
clue_num                                               int             comment '实际线索量',
free_clue_num                                          int             comment '免费线索量',
paid_clue_num                                          int             comment '付费线索量',
paid_clue_money                                        double          comment '付费线索成本',
total_cost                                             double          comment '消费总额',
flow_status                                            int             comment '记录主表当时流量状态,1:无意向,2:高意向,3:走合同中,4:出款中,5:已出款,6:投放中,7:投放结束',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_project_follow_flow_detail'
;
