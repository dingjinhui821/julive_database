drop table if exists ods.yw_sys_number_sms;
create external table ods.yw_sys_number_sms(
id                                                     int             comment '',
creator                                                bigint          comment '',
create_datetime                                        int             comment '',
updator                                                bigint          comment '',
update_datetime                                        int             comment '',
unite_order_id                                         bigint          comment '统一订单id',
order_id                                               bigint          comment '订单id',
esf_buy_order_id                                       bigint          comment '二手房买方订单id',
esf_sale_order_id                                      bigint          comment '二手房卖方订单id',
corp_key                                               string          comment '移客通企业id',
ts                                                     string          comment '接口请求的unix时间戳',
sign                                                   string          comment '签名',
sms_id                                                 string          comment '唯一标识id',
sender                                                 string          comment '发送者真实号码',
receiver                                               string          comment '接收者真实号码',
sender_show                                            string          comment '发送者分配号码',
receiver_show                                          string          comment '接收者分配号码',
transfer_time                                          string          comment '短信发送时刻',
sms_content                                            string          comment '短信内容',
sms_result                                             string          comment '短信发送结果',
update_by_ykt                                          int             comment '是否被移克通更新过 1:未更新，2:更新过',
fail_julive                                            string          comment '失败原因,居里写的',
source                                                 int             comment '1:移克通,2:居理自研',
employee_id_in_token                                   bigint          comment 'token得employee_id只针对esa系统',
employee_id                                            bigint          comment '咨询师id',
type                                                   int             comment '1:普通短信,2:彩信',
mms_id                                                 string          comment '彩信id,唯一',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_sys_number_sms'
;
