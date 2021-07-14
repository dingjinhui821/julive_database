drop table if exists ods.bd_house_type_imgs;
create external table ods.bd_house_type_imgs(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
room_type                                              int             comment '',
project_id                                             bigint          comment '楼盘id',
house_type_id                                          bigint          comment '户型id',
apart_img                                              string          comment '户型图片',
show_index                                             int             comment '展示排序',
status                                                 int             comment '1:假删除 2:待审核 3显示',
storey                                                 int             comment '楼层',
apart_img_path                                         string          comment '下载后的图片地址',
img_h                                                  string          comment '图片高',
img_w                                                  string          comment '图片宽',
download_datetime                                      int             comment '下载时间',
parent_house_type_img_id                               int             comment '父户型图id',
state                                                  int             comment '0:未处理1:已处理2:非法url 4:上传失败',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_house_type_imgs'
;
