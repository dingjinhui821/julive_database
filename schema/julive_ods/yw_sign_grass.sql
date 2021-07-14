drop table if exists ods.yw_sign_grass;
create external table ods.yw_sign_grass(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
order_id                                               bigint          comment '订单id',
employee_id                                            bigint          comment '员工id',
user_id                                                bigint          comment '用户id',
sign_name                                              string          comment '签约人姓名',
sign_phone                                             string          comment '签约人电话',
sign_identity_number                                   string          comment '签约人身份证号',
project_id                                             bigint          comment '楼盘id',
project_name                                           string          comment '楼盘名称',
house_type                                             int             comment '户型',
acreage                                                double          comment '面积',
house_number                                           string          comment '房号',
sign_datetime                                          int             comment '废弃字段:签约时间',
deal_money                                             int             comment '成交金额',
plan_commission_datetime                               int             comment '预计结佣时间',
note                                                   string          comment '备注',
status                                                 int             comment '3:已草签 4:退草签 -1:删除',
subscribe_id                                           bigint          comment '认购id，表示从哪个认购记录发起的签约',
sales_name                                             string          comment '销售姓名',
sales_mobile                                           string          comment '销售电话',
payment                                                string          comment '付款方式 1全款、2商贷、3公积金贷、4组合贷、5分期，',
full_payment_datetime                                  int             comment '预计全款到帐日期',
grass_sign_datetime                                    int             comment '草签时间',
is_cooperate                                           int             comment '是否是合作楼盘，1:是  2:否',
submit_review_datetime                                 int             comment '提交审核时间',
review                                                 int             comment '审核状态:0未审核，1审核成功，2审核失败，3审核中',
sign_img                                               string          comment '签约单照片',
reason                                                 string          comment '拒绝原因',
contract_img                                           string          comment '合同照片',
through_datetime                                       int             comment '审核通过时间',
sign_employee_id                                       string          comment '签约服务人id，多个用,隔开',
sign_type                                              int             comment '成交类型:0未知 1合作 2非合作 3无效 4外联 5待定',
sign_invalid_reason                                    int             comment '无效签约原因:1合作楼盘被跳单 2合作楼盘客户无效 3非合作楼盘未预约 4非合作楼盘未带看合作 5其他',
prompt_msg                                             string          comment '2种情况展示提示信息:1.签约单跟认购单重点字段不一致 2.签约类型与楼盘合作状态不一致',
audit_group_id                                         string          comment '每次提交审核的组id',
expand_employee_id                                     bigint          comment '拓展部人员id（报备人）',
expand_employee_name                                   string          comment '拓展部人员姓名（报备人）',
id_type                                                int             comment '证件类型: 1:大陆身份证  2:其他',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
deal_id                                                bigint          comment '成交id',
is_authorized                                          int             comment '用户是否授权:0未授权1已授权',
chargeback_reason                                      string          comment '退草签原因',
sign_possibility                                       int             comment '签约可能性 1高 2中 3低',
sign_possibility_reason                                string          comment '签约可能性原因说明',
plan_sign_datetime                                     int             comment '预计签约时间',
plan_sign                                              int             comment '预计是否会签约 0:否 1:是',
first_percent                                          double          comment '首付比例',
first_money                                            int             comment '首付金额',
plan_live_date                                         string          comment '预计交房时间',
see_project_list_id                                    bigint          comment 'see_project_list 表主键id',
see_project_id                                         bigint          comment '带看id',
is_straight                                            int             comment '成交方式 1直接成交 (从带看过来) 2转化成交(排号/认购过来的) 3无带看成交  4补录成交',
is_sms                                                 int             comment '是否发送过签约前提醒:1未发送2已发送',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_sign_grass'
;
