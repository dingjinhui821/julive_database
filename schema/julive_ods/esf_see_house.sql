drop table if exists ods.esf_see_house;
create external table ods.esf_see_house(
id                                                     bigint          comment '二手房买方订单带看id',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
order_id                                               bigint          comment '二手房买方订单id',
follow_up_id                                           bigint          comment '二手房买方订单跟进id',
employee_id                                            bigint          comment '所属咨询师id',
invitation_employee_id                                 bigint          comment '邀约人id',
see_employee_id                                        bigint          comment '带看员工id',
see_band_employee_id                                   bigint          comment '跟带人',
see_datetime                                           int             comment '计划/实际 带看开始时间',
status                                                 int             comment '1预约2实际',
city_id                                                int             comment '城市id',
record                                                 string          comment '带看记录',
meet_address                                           string          comment '约见地点',
meet_lat                                               string          comment '约见地点经纬度',
meet_lng                                               string          comment '约见地点经度',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/esf_see_house'
;
