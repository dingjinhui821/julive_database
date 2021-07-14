drop table if exists ods.yw_invoice_deal;
create external table ods.yw_invoice_deal(
id                                                     int             comment '',
deal_id                                                int             comment '成交单id',
money                                                  double          comment '本次开票金额',
invoice_id                                             int             comment '发票id',
create_datetime                                        int             comment '创建人',
update_datetime                                        int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_invoice_deal'
;
