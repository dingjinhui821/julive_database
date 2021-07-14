drop table if exists ods.activity_jd_card_record;
create external table ods.activity_jd_card_record(
id                                                     bigint          comment '',
city_id                                                int             comment '城市id',
order_id                                               bigint          comment '订单id',
jd_order_id                                            string          comment '京东订单id',
sign_id                                                bigint          comment '签约id',
comment_id                                             bigint          comment '邀约评价id',
user_id                                                bigint          comment '用户id',
user_name                                              string          comment '用户姓名',
user_mobile                                            string          comment '用户手机号',
share_status                                           int             comment '分享状态，1分享，2未分享',
expected_card_amount                                   int             comment '预计发卡金额',
send_card_status                                       int             comment '发卡状态，1发卡，2未发卡，3制卡中,4已取卡',
make_card_datetime                                     int             comment '制卡时间',
send_card_datetime                                     int             comment '发卡时间',
card_number                                            string          comment 'e卡卡号',
card_passwd                                            string          comment '明文的卡密',
card_password                                          string          comment '卡密',
card_amount                                            int             comment 'e卡金额',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
appeal_employee_id                                     bigint          comment '申诉员工',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/activity_jd_card_record'
;
