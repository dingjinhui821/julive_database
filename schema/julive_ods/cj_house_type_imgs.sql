drop table if exists ods.cj_house_type_imgs;
create external table ods.cj_house_type_imgs(
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
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_house_type_imgs'
;
