drop table if exists ods.esf_see_house_list;
create external table ods.esf_see_house_list(
id                                                     bigint          comment '二手房买方订单带看房源列表id',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
order_id                                               bigint          comment '二手房买方订单id',
follow_up_id                                           bigint          comment '二手房买方订单跟进id',
see_house_id                                           bigint          comment '二手房买方订单带看id',
see_employee_id                                        bigint          comment '带看员工id',
esf_village_id                                         int             comment '二手房小区id',
type                                                   int             comment '1已委托房源2已确认未委托房源3未确定房源',
esf_house_id                                           bigint          comment '二手房房源id',
building_num                                           string          comment '楼栋',
unit                                                   string          comment '单元',
room_number                                            string          comment '房号',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/esf_see_house_list'
;
