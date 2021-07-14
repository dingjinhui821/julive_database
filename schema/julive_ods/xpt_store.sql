drop table if exists ods.xpt_store;
create external table ods.xpt_store(
id                                                     int             comment '自增id',
store_name                                             string          comment '门店名称',
service_head                                           bigint          comment '经服负责人',
expand_head                                            bigint          comment '经拓负责人',
brand_id                                               int             comment '所属品牌商',
address                                                string          comment '地址描述',
lng                                                    string          comment '门店经度',
lat                                                    string          comment '门店纬度',
license_id                                             int             comment '营业执照id',
status                                                 int             comment '门店状态 1预录入，2营业中，3已关闭',
city_id                                                int             comment '门店城市',
business_scope                                         string          comment '经营范围 1二手房 2新房',
old_business_area                                      int             comment '二手房主营区域',
new_business_area                                      int             comment '新房主营区域',
is_delete                                              int             comment '是否删除 1是 2否',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '修改时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_store'
;
