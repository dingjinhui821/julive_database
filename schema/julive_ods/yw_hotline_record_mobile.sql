drop table if exists ods.yw_hotline_record_mobile;
create external table ods.yw_hotline_record_mobile(
id                                                     bigint          comment '',
mobile                                                 string          comment '外呼号码',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
use_datetime                                           int             comment '使用时间',
is_show                                                bigint          comment '1.显示，2.删除',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '修改时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_hotline_record_mobile'
;
