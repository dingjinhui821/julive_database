drop table if exists ods.yw_push_reg_id;
create external table ods.yw_push_reg_id(
id                                                     bigint          comment '自增id',
unique_id                                              string          comment 'cj_device_info.unique_id,设备的唯一标识',
app_id                                                 int             comment '居理对app的编号',
brand                                                  string          comment '手机品牌',
type                                                   int             comment '1:小米,2:华为,3:极光',
reg_id                                                 string          comment '各厂商生成的regid',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
unique_id_type                                         int             comment '1:服务端生成,2:客端生成的',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_push_reg_id'
;
