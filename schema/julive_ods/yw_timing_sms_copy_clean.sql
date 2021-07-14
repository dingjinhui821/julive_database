drop table if exists julive_bak.yw_timing_sms_copy_clean;
create external table julive_bak.yw_timing_sms_copy_clean(
id                                                     bigint          comment '主键id',
receive_phone                                          string          comment '短信接收人手机号',
text                                                   string          comment '短信文本',
remind_datetime                                        int             comment '提醒时间',
create_datetime                                        int             comment '记录创建时间',
update_datetime                                        int             comment '记录更新时间',
send_man                                               string          comment '短信发送者',
status                                                 int             comment '短信状态:-1已删除，0未发送，1已发送',
sms_type                                               int             comment '短信类型',
handle_id                                              bigint          comment '业务id',
tpl                                                    string          comment '短信模板关键字',
var_array                                              string          comment '要替换短信模板中变量的实际值',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/julive_bak/yw_timing_sms_copy_clean'
;
