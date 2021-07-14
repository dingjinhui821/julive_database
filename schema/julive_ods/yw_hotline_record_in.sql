drop table if exists ods.yw_hotline_record_in;
create external table ods.yw_hotline_record_in(
id                                                     bigint          comment '',
unique_id                                              string          comment '通话记录唯一标识',
enterprise_id                                          string          comment '企业id',
customer_number                                        string          comment '客户号码',
customer_area_code                                     string          comment '呼入或外呼座席接听后的座席区号',
customer_number_type                                   int             comment '来电或外呼客户号码类型: 2手机 1固话 0未知',
hotline                                                string          comment '热线号码',
crm_id                                                 string          comment '座席crm id',
cno                                                    string          comment '座席工号',
pwd                                                    string          comment '座席密码',
bind_tel                                               string          comment '绑定电话',
call_type                                              int             comment '呼叫类型:1呼入 2web400呼入',
number_trunk                                           string          comment '中继号码',
task_inventory_id                                      string          comment '任务记录id',
task_id                                                string          comment '任务id',
qid                                                    string          comment '队列id',
consulter_cno                                          string          comment '咨询座席号',
transfer_cno                                           string          comment '转移座席号',
remote_ip                                              string          comment '调用方ip',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
status                                                 int             comment '状态:0未知 1来电响铃 2已接通 3未接通 4挂机',
cdr_start_time                                         int             comment '进入系统时间',
cdr_answer_time                                        int             comment '系统接听时间',
cdr_end_time                                           int             comment '挂机时间',
cdr_status                                             int             comment '通话状态:1座席接听 2已呼叫座席座席未接听 3系统接听 4系统未接-ivr配置错误 5系统未接-停机 6系统未接-欠费 7系统未接-黑名单 8系统未接-未注册 9系统未接-彩铃 10网上400未接受 11系统未接-呼叫超出营帐中设置的最大限制 12其他错误',
cname                                                  string          comment '客服姓名',
wait_secs                                              int             comment '等待时长(秒)',
conn_secs                                              int             comment '通话时长(秒)',
all_secs                                               int             comment '总时长(秒)',
result                                                 string          comment '呼叫结果',
ivr_secs                                               int             comment 'ivr时长(秒)',
city_id                                                bigint          comment '城市',
order_id                                               bigint          comment '对应的订单id',
end_type                                               int             comment '1: 其它（除11,12,13之前的）11用户挂断,12坐席挂断 13转接',
job_number                                             string          comment '员工工号',
satisfy_code                                           int             comment '1=满意2=服务态度差3=造成打扰4=未解答问题-1:用户挂机-2:超时未评价-3:未评价挂机-4:坐席挂机0:初始默认',
show_number                                            string          comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_hotline_record_in'
;
