drop table if exists ods.cj_project_developer;
create external table ods.cj_project_developer(
id                                                     int             comment 'id',
city_id                                                int             comment '城市id',
project_id                                             bigint          comment '楼盘id',
project_name                                           string          comment '楼盘名称',
developer_name                                         string          comment '开发商名称',
project_phone                                          string          comment '项目电话',
saler_name                                             string          comment '销售名称',
saler_phone                                            string          comment '销售电话',
saler_img                                              string          comment '销售照片',
qrcode_img                                             string          comment '楼盘详情页二维码',
poster_img                                             string          comment '海报图片',
channel_id                                             int             comment '渠道id',
developer_id                                           int             comment '开发商id',
creator                                                int             comment '创建者id',
updator                                                int             comment '更新者id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_developer'
;
