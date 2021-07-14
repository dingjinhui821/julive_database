drop table if exists ods.bd_license;
create external table ods.bd_license(
id                                                     int             comment '',
license_type                                           int             comment '证件类型 1销售许可证 2预售许可证',
license_number                                         string          comment '证件号',
note                                                   string          comment '备注',
cj_license_id                                          int             comment '居理许可证id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
issue_datetime                                         bigint          comment '发证日期',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
city_id                                                int             comment '城市id',
source_num                                             int             comment '来源 1搜房 2安居客 3搜狐',
license_number_arab                                    bigint          comment '证件号-数字',
district_id                                            int             comment '区域id',
is_task                                                int             comment '是否有任务 1 是 2 否',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_license'
;
