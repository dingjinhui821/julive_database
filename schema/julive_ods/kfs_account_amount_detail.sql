drop table if exists ods.kfs_account_amount_detail;
create external table ods.kfs_account_amount_detail(
id                                                     int             comment '主键id',
developer_id                                           int             comment '开发商id',
project_id                                             int             comment '楼盘id',
source                                                 int             comment '来源 1充值 2退款',
op_amount                                              double          comment '操作金额',
op_datetime                                            int             comment '操作时间',
returnable_amount                                      double          comment '可退金额',
balance_amount                                         double          comment '该条记录剩余余额',
incr_amount                                            string          comment '账户余额和可退金额增量json',
amount                                                 double          comment '到账金额',
rebate                                                 int             comment '返点',
is_delete                                              int             comment '1未删除 2.已删除',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_account_amount_detail'
;
