drop table if exists ods.xpt_house_img;
create external table ods.xpt_house_img(
id                                                     int             comment '自增id',
house_id                                               bigint          comment '房源id',
url                                                    string          comment '图片地址',
type                                                   int             comment '类型 1主卧、2次卧、3客厅、4餐厅、5卫生间、6衣帽间、7厨房、8阳台，9户型图',
weight                                                 int             comment '权重',
is_index                                               int             comment '是否为首图 1是 2否',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '修改时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_house_img'
;
