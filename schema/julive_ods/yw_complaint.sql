drop table if exists ods.yw_complaint;
create external table ods.yw_complaint(
id                                                     int             comment '',
order_id                                               int             comment '订单id',
call_datetime                                          int             comment '投诉时间',
complaint_employee_id                                  int             comment '投诉咨询师id',
complaint_info                                         string          comment '投诉原因/内容',
create_datetime                                        int             comment '记录创建时间',
update_datetime                                        int             comment '记录更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_complaint'
;
