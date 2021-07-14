drop table if exists ods.dsp_account_rebate;
create external table ods.dsp_account_rebate(
id                                                     int             comment '',
start_date                                             int             comment '返点开始执行日期，存储时间戳',
end_date                                               int             comment '返点结束时间 存时间戳',
dsp_account_id                                         int             comment 'dsp账户id',
rebate                                                 double          comment '返点',
is_repair_history                                      int             comment '是否修复历史数据，1:已修复或者不需要修复 2 未修复  当管理员修改返点后，会重新计算消费信息',
remark                                                 string          comment '备注',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
status                                                 int             comment '状态 1启用 0禁用',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_account_rebate'
;
