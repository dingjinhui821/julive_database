drop table if exists ods.bd_license_img;
create external table ods.bd_license_img(
id                                                     int             comment 'id',
license_id                                             bigint          comment '许可证id',
url                                                    string          comment '图片源地址',
img_url                                                string          comment '图片本地地址',
project_id                                             bigint          comment '楼盘id',
download_datetime                                      int             comment '图片下载时间',
state                                                  int             comment '0:未处理1:已处理2:非法url3:下载失败4:上传失败5:宽高获取失败6:下载成功待上传阿里云',
watermark                                              string          comment '水印',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_license_img'
;
