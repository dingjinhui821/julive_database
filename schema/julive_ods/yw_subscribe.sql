drop table if exists ods.yw_subscribe;
create external table ods.yw_subscribe(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
order_id                                               bigint          comment '订单id',
employee_id                                            bigint          comment '员工id',
user_id                                                bigint          comment '用户id',
subscribe_name                                         string          comment '认购人姓名',
subscribe_phone                                        string          comment '认购人电话',
subscribe_identity_number                              string          comment '认购人身份证号',
project_id                                             bigint          comment '楼盘id',
project_name                                           string          comment '楼盘名称',
house_type                                             int             comment '户型',
acreage                                                double          comment '面积',
house_number                                           string          comment '房号',
subscribe_datetime                                     int             comment '认购时间',
deal_money                                             int             comment '成交金额',
plan_sign_datetime                                     int             comment '预计签约时间',
note                                                   string          comment '备注',
status                                                 int             comment '认购状态: 1:已认购  2:退认购  3:排号   4退排号   -1:逻辑删除',
subscri_book_sign_datetime                             int             comment '认购书签约时间',
sales_name                                             string          comment '销售姓名',
sales_mobile                                           string          comment '销售电话',
row_number_datetime                                    int             comment '排号时间',
is_cooperate                                           int             comment '是否是合作楼盘，1:是  2:否',
see_project_list_id                                    bigint          comment '带看id',
review                                                 int             comment '审核状态:0未审核，1审核成功，2审核失败，3审核中',
submit_review_datetime                                 int             comment '提交审核时间',
reason                                                 string          comment '拒绝原因',
subscribe_img                                          string          comment '排号单/认购单照片',
through_datetime                                       int             comment '审核通过时间',
first_pay_datetime                                     int             comment '定房时间',
subscribe_type                                         int             comment '成交类型:0未知 1合作 2非合作 3无效 4外联 5待定',
subscribe_invalid_reason                               int             comment '无效认购原因:1合作楼盘被跳单 2合作楼盘客户无效 3非合作楼盘未预约 4非合作楼盘未带看合作 5其他',
prompt_msg                                             string          comment '2种情况展示提示信息:1.认购类型与楼盘合作状态不一致 2.非合作楼盘发起认购',
type_subscribe                                         int             comment '发起认购的方式:1带看转认购 2排号转认购',
audit_group_id                                         string          comment '每次提交审核的组id',
expand_employee_id                                     bigint          comment '拓展部人员id（报备人）',
expand_employee_name                                   string          comment '拓展部人员姓名（报备人）',
id_type                                                int             comment '证件类型: 1:大陆身份证  2:其他',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
deal_id                                                bigint          comment '成交id',
sign_possibility                                       int             comment '签约可能性 1高 2中 3低',
employee_manager_id                                    int             comment '咨询师主管id',
employee_manager_name                                  string          comment '咨询师主管姓名',
chargeback_reason                                      string          comment '退认购原因',
sign_possibility_reason                                string          comment '签约可能性原因说明',
payment                                                string          comment '付款方式 1全款、2商贷、3公积金贷、4组合贷、5分期，',
plan_sign                                              int             comment '预计是否会签约 0:否 1:是',
subscribed_amount                                      int             comment '认购金额',
discount_content                                       string          comment '优惠内容',
payment_expected_time                                  int             comment '剩余首付预计交期时间',
see_project_id                                         int             comment '带看id',
is_straight                                            int             comment '成交方式 1直接成交 (从带看过来) 2转化成交(排号/认购过来的) 3无带看成交  4补录成交',
is_sms                                                 int             comment '是否发送过签约前提醒:1未发送2已发送',
first_percent                                          double          comment '首付比例',
pay_type                                               int             comment '支付途径（1.远程支付、2.售楼处支付',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_subscribe'
;
