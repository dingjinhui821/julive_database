drop table if exists ods.dsp_app_data_import_data;
create external table ods.dsp_app_data_import_data(
id                                                     int             comment '主键',
record_date                                            int             comment '日期',
media_type                                             int             comment '媒体类型 常量值',
product_type                                           int             comment '产品形态（feed:1sem:4app:3）',
account_id                                             int             comment '账户id',
account_name                                           string          comment '账户名',
app_type                                               int             comment 'app类型，0非app 1 苹果 2 安卓',
city_name                                              string          comment '城市名称',
cash_cost                                              double          comment '总消耗',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_app_data_import_data'
;
