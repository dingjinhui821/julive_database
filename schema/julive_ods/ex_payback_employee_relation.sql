drop table if exists ods.ex_payback_employee_relation;
create external table ods.ex_payback_employee_relation(
id                                                     bigint          comment 'id',
payback_employee_id                                    int             comment '回款负责人id',
business_type                                          int             comment '类型:1.结佣合同2.成交单',
business_id                                            int             comment 'int',
create_datetime                                        int             comment '创建时间',
creator                                                int             comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_payback_employee_relation'
;
