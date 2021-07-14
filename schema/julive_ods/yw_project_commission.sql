drop table if exists ods.yw_project_commission;
create external table ods.yw_project_commission(
id                                                     bigint          comment '',
create_datetime                                        int             comment '创建时间',
creator                                                bigint          comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                bigint          comment '更新人',
project_id                                             bigint          comment '楼盘id',
contract_begin_datetime                                int             comment '合同开始日期',
contract_end_datetime                                  int             comment '合同结束日期',
valid_begin_datetime                                   int             comment '有效期开始日期',
valid_end_datetime                                     int             comment '有效期结束日期',
commission_rule                                        string          comment '结佣规则',
status                                                 int             comment '状态 1有效  2即将到期  3无效',
status_flag                                            int             comment '当前日期与有效期相差的天数  <=30 即将到期,<=0 无效',
business_order_id                                      int             comment '关联的 bd 单 id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_project_commission'
;
