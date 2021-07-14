drop table if exists ods.yw_enum;
create external table ods.yw_enum(
id                                                     int             comment '',
`table`                                                string          comment '数据库表',
field                                                  string          comment '表字段',
enum_id                                                string          comment '枚举id',
enum_name                                              string          comment '枚举代表的名称',
update_datetime                                        int             comment '',
create_datetime                                        int             comment '',
create_at                                              string          comment '添加人',
update_at                                              string          comment '维护人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_enum'
;
