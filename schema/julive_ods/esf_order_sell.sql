drop table if exists ods.esf_order_sell;
create external table ods.esf_order_sell(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
city_id                                                int             comment '城市id',
op_type                                                int             comment '',
channel_id                                             int             comment '对应渠道表的id',
channel_put                                            string          comment '渠道投放关键词',
source                                                 int             comment '订单了解途径',
device_type                                            int             comment '设备来源:1pc，2移动，3android，4ios，5未知',
insert_mobile_time                                     int             comment '留电时间',
user_id                                                bigint          comment '用户id',
user_realname                                          string          comment '用户姓名',
user_mobile                                            string          comment '用户手机号',
more_mobile                                            string          comment '备用联系方式',
sex                                                    int             comment '1:男2:女',
status                                                 int             comment '',
intent_employee_id                                     bigint          comment '意向咨询师id',
intent_employee_name                                   string          comment '意向咨询师姓名',
new_house_order_id                                     bigint          comment '新房订单id',
employee_id                                            bigint          comment '所属咨询师id',
is_close                                               int             comment '是否关闭:1是，2否',
close_order_reason                                     int             comment '关闭订单原因',
close_order_explan                                     string          comment '关闭订单原因说明',
close_order_datetime                                   int             comment '关闭订单时间',
trust_house_id                                         string          comment '二手房卖方侧房源id,多个逗号隔开',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/esf_order_sell'
;
