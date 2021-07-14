drop table if exists ods.zp_files;
create external table ods.zp_files(
id                                                     int             comment '',
file_id                                                string          comment '上传文件id',
link_id                                                int             comment '相关业务id',
file_type                                              int             comment '文件类型 1 图片 2录音 3 pdf 4 doc 5 other 6 html',
file_url                                               string          comment '文件地址',
status                                                 int             comment '文件是否删除 1、是 2 否',
name                                                   string          comment '文件名称',
file_size                                              string          comment '文件尺寸(单位b)',
type                                                   int             comment '业务类型 1、电话面试 2 一面 3 二面 4 附件简历 5 offer',
creator                                                int             comment '创建人id',
updator                                                int             comment '更新人id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/zp_files'
;
