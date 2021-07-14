drop table if exists ods.kfs_order_follow;
create external table ods.kfs_order_follow(
id                                                     int             comment '主键id',
order_id                                               int             comment '开发商订单id',
clue_feedback                                          int             comment '线索反馈,1打不通,2不考虑买房,3区域不符合,4预算不符合,5资质不符合,6可持续跟进,7高意向可到访',
is_see                                                 int             comment '默认是否,1是,2否',
is_subscribe                                           int             comment '是否认购,默认是否,1是,2否',
remark                                                 string          comment '具体描述',
creator                                                int             comment '创建人',
create_datetime                                        int             comment '创建时间',
updator                                                int             comment '更新人',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_order_follow'
;
