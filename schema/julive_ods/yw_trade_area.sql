drop table if exists ods.yw_trade_area;
create external table ods.yw_trade_area(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
trade_area                                             string          comment '区域板块名称',
pinyin                                                 string          comment '板块拼音',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
district_id                                            string          comment '区域id',
plate_about                                            string          comment '板块介绍',
plate_status                                           int             comment '1: 有效  2:暂停',
district_name                                          string          comment '区域名称',
is_important_plate                                     int             comment '是否是重点板块 1: 是  2:不是',
show_index                                             int             comment '显示顺序',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_trade_area'
;
