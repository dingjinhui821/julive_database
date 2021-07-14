drop table if exists ods.kfs_account_amount_info;
create external table ods.kfs_account_amount_info(
id                                                     int             comment '主键id',
developer_id                                           int             comment '开发商id',
project_id                                             int             comment '楼盘id',
returnable_amount                                      double          comment '可退金额',
account_balance_amount                                 double          comment '账户余额',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_account_amount_info'
;
