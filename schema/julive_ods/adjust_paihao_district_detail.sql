drop table if exists ods.adjust_paihao_district_detail;
create external table ods.adjust_paihao_district_detail(
paihao_id                                              bigint          comment '排号id',
district_id                                            bigint          comment '区域id',
value                                                  double          comment '核算量(最高1)',
happen_updatetime                                      int             comment '业务发生时间',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
is_subscribe                                           int             comment '是否进入认购,0否1是',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/adjust_paihao_district_detail'
;
