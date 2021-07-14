drop table if exists ods.adjust_see_project_channel_detail;
create external table ods.adjust_see_project_channel_detail(
see_project_id                                         bigint          comment '带看id',
channel_id                                             bigint          comment '渠道id',
value                                                  double          comment '核算量(最高1)',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
happen_updatetime                                      int             comment '业务发生时间',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/adjust_see_project_channel_detail'
;
