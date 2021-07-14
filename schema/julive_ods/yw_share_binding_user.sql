drop table if exists ods.yw_share_binding_user;
create external table ods.yw_share_binding_user(
id                                                     int             comment '自增id',
user_id                                                bigint          comment '用户id',
mobile                                                 string          comment '手机号码',
employee_id                                            bigint          comment '咨询师id',
city_id                                                int             comment '城市id',
share_id                                               int             comment '资料id',
share_type                                             int             comment '资料类型1:单楼盘2:多楼盘3:文章',
open_id                                                string          comment 'open_id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
browse_datetime                                        int             comment '浏览资料的时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_binding_user'
;
