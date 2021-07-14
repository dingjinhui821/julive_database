drop table if exists ods.view_subscribe_identity_parse;
create external table ods.view_subscribe_identity_parse(
id                                                     bigint          comment '',
identity_place                                         string          comment '',
identity_birthday                                      string          comment '',
identity_sex                                           string          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/view_subscribe_identity_parse'
;
