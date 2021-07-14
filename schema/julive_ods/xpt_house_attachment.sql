drop table if exists ods.xpt_house_attachment;
create external table ods.xpt_house_attachment(
id                                                     bigint          comment '自增id',
house_id                                               bigint          comment '房源id',
file_type                                              int             comment '文件类型 1 图片 2 文件 3房源视频',
file_url                                               string          comment '文件地址',
file_name                                              string          comment '原文件名称',
file_size                                              string          comment '原文件大小',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '修改时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_house_attachment'
;
