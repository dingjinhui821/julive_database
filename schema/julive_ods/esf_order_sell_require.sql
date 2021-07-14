drop table if exists ods.esf_order_sell_require;
create external table ods.esf_order_sell_require(
id                                                     bigint          comment '二手房买方订单需求id',
order_id                                               bigint          comment '订单id',
village_id                                             int             comment '小区id',
village_name                                           string          comment '小区名',
acreage                                                double          comment '面积',
leave_phone_hope_price                                 double          comment '留电期望售价',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
leave_phone_village_id                                 int             comment '留电小区id',
leave_phone_village_name                               string          comment '留电小区名',
leave_phone_acreage                                    double          comment '留电面积',
district_id                                            int             comment '区域',
hope_price_min                                         double          comment '期望售价最小值',
hope_price_max                                         double          comment '期望售价最大值',
building_num                                           string          comment '楼栋',
unit                                                   string          comment '单元',
room_number                                            string          comment '门牌号',
floor                                                  string          comment '所在楼层',
room_count                                             int             comment '室',
hall                                                   int             comment '厅',
toilet                                                 int             comment '卫',
server_note                                            string          comment '客服备注',
hope_contact_time                                      int             comment '客服期望联系时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/esf_order_sell_require'
;
