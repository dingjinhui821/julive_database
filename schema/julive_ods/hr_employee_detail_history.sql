drop table if exists ods.hr_employee_detail_history;
create external table ods.hr_employee_detail_history(
id                                                     bigint          comment '表id',
detail_id                                              bigint          comment '详情id',
employee_id                                            int             comment '员工id',
country                                                int             comment '（基本信息）国家/地区',
idcard                                                 string          comment '（基本信息）身份证号',
idcardimg_back                                         string          comment '证件照（反）',
idcardimg_backroute                                    string          comment '证件反面路径',
idcardimg_route                                        string          comment '（基本信息）证件照路径',
idcardimg                                              string          comment '（基本信息）身份证照片（正）',
nation                                                 string          comment '（基本信息）民族',
english_name                                           string          comment '（基本信息）英文名',
marital_status                                         int             comment '（基本信息）婚姻状况',
photos_route                                           string          comment '（基本信息）员工照片路径',
photos                                                 string          comment '（基本信息）员工照片',
constellation                                          int             comment '（基本信息）星座',
zodiac                                                 int             comment '（基本信息）属相 1:猴 2:鸡 3:狗 4:猪 5:鼠 6:牛 7:虎 8:兔 9:龙 10:蛇 11:马 12:羊',
blood_type                                             string          comment '（基本信息）血型',
domicile_address                                       string          comment '（基本信息）户籍所在地',
political_outlook                                      string          comment '（基本信息）政治面貌',
join_partytime                                         int             comment '（基本信息）入党时间',
organization                                           string          comment '（基本信息）存档机构',
child_status                                           string          comment '（基本信息）子女状态',
insurance                                              int             comment '（基本信息）子女有无商业保险',
law                                                    string          comment '（基本信息）有无违法违纪行为',
illness                                                string          comment '（基本信息）有无重大病史',
qq                                                     int             comment '(通讯信息) qq',
weixin                                                 int             comment '(通讯信息)微信',
city                                                   string          comment '(通讯信息)通讯城市',
begin_permitime                                        int             comment '(通讯信息)居住证办理日期',
end_permitime                                          int             comment '(通讯信息)居住证截止日期',
now_addrress                                           string          comment '(通讯信息)现居住地',
phone_address                                          string          comment '(通讯信息)通讯地址',
bei_phone                                              bigint          comment '（通讯信息）备用联系方式',
urgent_man                                             string          comment '(通讯信息)紧急联系人',
urgent_phone                                           string          comment '(通讯信息)紧急联系电话',
security_num                                           string          comment '(账号信息)社保电脑号',
fund_num                                               string          comment '(账号信息)社保账号',
bank_num                                               bigint          comment '(账号信息)银行卡号',
open_bank                                              string          comment '(账号信息)开户行',
high_major                                             int             comment '（教育信息）最高学历',
entrance_time                                          int             comment '(教育信息)入学时间',
graduation_time                                        int             comment '（教育信息）毕业时间',
major                                                  string          comment '(教育信息)专业',
diploma_route                                          string          comment '（教育信息）毕业证书路径',
diploma                                                string          comment '（教育信息）毕业证书',
degree_route                                           string          comment '（教育信息）学位证书路径',
degree                                                 string          comment '（教育信息）学位证书',
company                                                string          comment '（从业信息）最后受聘公司',
up_company                                             string          comment '（从业信息）上家公司',
title                                                  string          comment '（从业信息）职称',
attachment                                             string          comment '简历地址',
resume                                                 string          comment '（从业信息）简历',
competition                                            string          comment '（从业信息）有无竞业限制',
note                                                   string          comment '（从业信息）备注',
contract                                               string          comment '（自定义信息）合同归属',
ascription                                             string          comment '（自定义信息）社保及公积金归属',
origin                                                 string          comment '（自定义信息）籍贯地址',
highschool                                             string          comment '（自定义信息）新最高学历',
entry_datetime                                         int             comment '入职时间',
report_leader                                          int             comment '汇报对象',
employment_form                                        int             comment '聘用形式',
management_form                                        int             comment '管理形式',
full_time                                              int             comment '转正时间',
document_type                                          int             comment '证件类型',
age                                                    int             comment '年龄',
transfer                                               int             comment '有外调意愿',
excel                                                  int             comment '擅长方面',
xue_type                                               int             comment '（教育信息）学历类型',
graduation_school                                      string          comment '（教育信息）毕业学校',
personnel_email                                        string          comment '个人邮箱',
relationship_me                                        int             comment '与本人关系',
`like`                                                 string          comment '兴趣爱好',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
pre_regular_datetime                                   bigint          comment '转正式时间(实习生转正式员工)',
full_type                                              int             comment '1 未转正 2已转正 3见习期未转正 4见习期已转正',
retry_job                                              int             comment '再入职标识 2',
school_attributes                                      string          comment '学校属性',
household_type                                         int             comment '户口类型 1-农业户口 2-非农业户口',
plan_full_time                                         int             comment '计划转正时间',
department_attributes                                  int             comment '1 支撑部门 2业务部门 3 职能部门',
intern_to_regular_time                                 int             comment '见习期转正时间',
is_charger                                             int             comment '是否是部门负责人 1是 2不是',
probationary_status                                    int             comment '见习期转正状态  1未转正 2已转正',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hr_employee_detail_history'
;
