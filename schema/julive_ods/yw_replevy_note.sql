drop table if exists ods.yw_replevy_note;
create external table ods.yw_replevy_note(
id                                                     bigint          comment 'id',
replevy_id                                             bigint          comment '业务追回id',
add_employee_id                                        bigint          comment '记录人id',
add_employee_name                                      string          comment '记录人姓名',
replevy_note                                           string          comment '追回记录',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_replevy_note'
;
