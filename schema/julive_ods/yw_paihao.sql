drop table if exists ods.yw_paihao;
create external table ods.yw_paihao(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
order_id                                               bigint          comment '订单id',
employee_id                                            bigint          comment '员工id',
user_id                                                bigint          comment '用户id',
user_name                                              string          comment '排号人姓名',
user_phone                                             string          comment '排号人电话',
user_identity_number                                   string          comment '排号人身份证号',
project_id                                             bigint          comment '楼盘id',
project_name                                           string          comment '楼盘名称',
house_type                                             int             comment '户型',
acreage                                                double          comment '面积',
house_number                                           string          comment '房号',
paihao_datetime                                        int             comment '排号时间',
deal_money                                             int             comment '成交金额',
plan_subscribe_datetime                                int             comment '预计认购时间',
note                                                   string          comment '备注',
status                                                 int             comment '排号状态: 3:已排号  4:退排号 -1:逻辑删除',
sales_name                                             string          comment '销售姓名',
sales_mobile                                           string          comment '销售电话',
is_cooperate                                           int             comment '是否是合作楼盘，1:是  2:否',
see_project_list_id                                    bigint          comment '带看id',
review                                                 int             comment '审核状态:0未审核，1审核成功，2审核失败，3审核中',
submit_review_datetime                                 int             comment '提交审核时间',
reason                                                 string          comment '拒绝原因',
paihao_img                                             string          comment '排号单照片',
through_datetime                                       int             comment '审核通过时间',
first_pay_datetime                                     int             comment '定房时间',
audit_group_id                                         string          comment '每次提交审核的组id',
expand_employee_id                                     bigint          comment '拓展部人员id（报备人）',
expand_employee_name                                   string          comment '拓展部人员姓名（报备人）',
id_type                                                int             comment '证件类型: 1:大陆身份证  2:其他',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
deal_id                                                bigint          comment '成交id',
employee_manager_id                                    int             comment '咨询师主管id',
employee_manager_name                                  string          comment '咨询师主管姓名',
deal_type                                              int             comment '成交类型0未知 1合作 2非合作 3无效 4外联 5待定',
chargeback_reason                                      string          comment '退排号原因',
subscribe_possibility                                  int             comment '认购可能性 1高 2中 3低',
subscribe_possibility_reason                           string          comment '认购可能性原因说明',
plan_subscribe                                         int             comment '预计是否会认购 0:否 1:是',
see_project_id                                         bigint          comment '带看id',
is_straight                                            int             comment '成交方式 1直接成交 (从带看过来) 2转化成交(排号/认购过来的) 3无带看成交 4补录成交',
pick_house_rule                                        string          comment '选房规则',
plan_open_time                                         string          comment '预计开盘时间',
discount                                               string          comment '排号优惠',
cost                                                   int             comment '排号费用',
plan_open_type                                         string          comment '预计开盘户型',
is_sms                                                 int             comment '是否发送过认购前提醒:1未发送2已发送',
first_percent                                          double          comment '首付比例',
pay_type                                               int             comment '支付途径（1.远程支付、2.售楼处支付）',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_paihao'
;