drop table if exists ods.kfs_project_follow_follower;
create external table ods.kfs_project_follow_follower(
id                                                     int             comment '主键id',
follow_id                                              int             comment '跟进主表id,对应kfs_project_follow表中id',
follower_type                                          int             comment '跟进人类型,1:渠道跟进人,2:客户成功跟进人',
follower_id                                            int             comment '跟进人id, 从hr库中的yw_employee表中来',
follower_job_number                                    int             comment '跟进人工号',
follower_name                                          string          comment '跟进人姓名,冗余字段,方便使用',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_project_follow_follower'
;
