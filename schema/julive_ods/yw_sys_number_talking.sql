drop table if exists ods.yw_sys_number_talking;
create external table ods.yw_sys_number_talking(
id                                                     int             comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
employee_sim_type                                      int             comment '咨询师号码运营商0:未知,咨询师没升级到1.4,1:是电信,2:其他运营商',
order_type                                             int             comment '1:正常业务订单通话，3:1v1的通话',
unite_order_id                                         bigint          comment '统一订单id',
order_id                                               bigint          comment '订单id',
esf_buy_order_id                                       bigint          comment '二手房买方订单id',
esf_sale_order_id                                      bigint          comment '二手房卖方订单id',
corp_key                                               string          comment '移客通企业id',
ts                                                     string          comment '接口请求的unix时间戳',
sign                                                   string          comment '签名',
recorder_id                                            string          comment '本次通话唯一标识',
caller_type                                            int             comment '1:主叫人是咨询师，2:主叫人是用户',
employee_id                                            bigint          comment '咨询师id',
caller                                                 string          comment '主叫真实号',
called                                                 string          comment '被叫真实号',
caller_show                                            string          comment '主叫分配号',
called_show                                            string          comment '被叫分配号',
caller_out_time                                        string          comment '呼出时间',
begin_time                                             string          comment '主叫拨通虚拟号时刻',
connect_time                                           string          comment '被叫接通时刻',
alerting_time                                          string          comment '被叫振铃时间',
release_time                                           string          comment '通话结束时刻',
call_duration                                          int             comment '通话时长，秒',
bill_duration                                          int             comment '计费时长，秒',
call_result                                            string          comment '通话状态',
record_file_url                                        string          comment '通话录音url',
record_file_aliyun_url                                 string          comment '通话记录在阿里云的地址',
call_cost                                              string          comment '本次通话费用，元',
caller_area                                            string          comment '主叫地区',
called_area                                            string          comment '被叫地区',
upload                                                 int             comment '1:未上传，2:已经上传',
fail                                                   string          comment '失败原因',
fail_julive                                            string          comment '失败原因,居里写的',
update_by_ykt                                          int             comment '是否被移刻通更新过1:没有,2:更新',
record_txt_json_url                                    string          comment '通话记录解析文本url',
record_manual_txt_json_url                             string          comment '人工修改的通话记录解析文本url',
employee_manager_id                                    bigint          comment '通话时的组长id',
ai_deal_tag                                            int             comment 'ai文本转化处理标记:0未处理，1已处理，2异常',
old_order_id                                           bigint          comment '因为合并订单,会更新原订单id',
source                                                 int             comment '1:移克通,2:居理自研',
insert_by                                              string          comment '是被那个接口插入的',
department_id                                          bigint          comment '通话时的组id',
audio_segment_status                                   int             comment '音频信息切割状态 0-未切割，1-已切割',
employee_id_in_token                                   bigint          comment 'token得employee_id只针对esa系统',
is_audio_upload                                        int             comment '录音是否上传:1否2是，备用字段，现阶段不采用，由ai组自己判断是否上传了二进制音频文件',
audio_upload_time                                      int             comment '录音上传时间',
update_note                                            string          comment '数据更新备注',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_sys_number_talking'
;
