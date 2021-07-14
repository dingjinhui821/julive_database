drop table if exists ods.cj_license_attachment;
create external table ods.cj_license_attachment(
id                                                     int             comment '',
license_id                                             int             comment '许可证id',
file_type                                              int             comment '文件类型 1图片类型 2非图片类型',
img_url                                                string          comment '图片地址',
old_name                                               string          comment '原文件名称',
file_size                                              string          comment '文件大小',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_license_attachment'
;
