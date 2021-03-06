drop table if exists ods.yw_order_require_history;
create external table ods.yw_order_require_history(
id                                                     bigint          comment '',
require_id                                             bigint          comment '订单需求id',
create_datetime                                        int             comment '记录创建时间',
creator                                                int             comment '记录创建人',
update_datetime                                        int             comment '订单需求更新时间',
updator                                                bigint          comment '订单需求更新人',
order_id                                               bigint          comment '订单id',
user_id                                                bigint          comment '用户id',
urgent                                                 int             comment '购房紧迫度, 1低 2中 3高',
cooperate                                              int             comment '配合度, 1低 2中 3高',
district_id                                            string          comment '区域id，逗号分隔',
district_other                                         string          comment '区域其他',
line                                                   string          comment '环线，逗号分隔',
line_status                                            int             comment '是否限制环线',
line_max                                               int             comment '最大环线',
subway                                                 string          comment '地铁线，逗号分隔',
address                                                string          comment '地址',
house_type                                             string          comment '户型，逗号分隔',
project_type                                           string          comment '业态类型',
acreage_min                                            int             comment '最小面积',
acreage_max                                            int             comment '最大面积',
total_price_min                                        int             comment '最小总价',
total_price_max                                        int             comment '最大总价',
unit_price_min                                         int             comment '最小单价',
unit_price_max                                         int             comment '最大单价',
first_price_min                                        int             comment '最小首付',
first_price_max                                        int             comment '最大首付',
month_price_min                                        int             comment '最小月供',
month_price_max                                        int             comment '最大月供',
interest_project                                       string          comment '客户感兴趣楼盘id，用逗号隔开',
recommend_project                                      string          comment '侃家推荐楼盘id，用逗号隔开',
saw_project                                            string          comment '之前看过楼盘id，用逗号隔开',
note                                                   string          comment '备注',
see_datetime                                           int             comment '用户看房时间',
interest_project_name                                  string          comment '客户感兴趣楼盘的名称，用逗号隔开',
recommend_project_name                                 string          comment '侃家推荐楼盘的名称，用逗号隔开',
saw_project_name                                       string          comment '之前看过楼盘的名称，用逗号隔开',
investment                                             int             comment '是否是投资',
qualifications                                         int             comment '是否有资质',
intent                                                 int             comment '1:无意向 2:保留 3:有意向',
intent_enddate                                         int             comment '意向保留截止日期',
intent_low_datetime                                    int             comment '变为无意向的时间',
intent_employee_id                                     bigint          comment '意向咨询师',
intent_employee_name                                   string          comment '意向咨询师',
intent_no_like                                         string          comment '无意向说明',
lowintent_reincustomer_datetime                        int             comment '无意向重新上户时间',
intent_low_reason                                      int             comment '关闭客户原因（即无意向原因）',
is_elite                                               int             comment '是否是高端客户（最高总结在600万以上的就是高端客户），1是，2否',
building_type                                          int             comment '建筑类型',
purchase_purpose                                       int             comment '购房目的:1刚需，2改善，3投资，4自助+投资',
purchase_urgency                                       int             comment '购房紧迫度:1高，2中，3低',
accept_other_area                                      int             comment '是否接受其它区域:1是，2否',
see_time                                               int             comment '可看房时间:1工作日，2周末，3随时，4其它',
see_time_desc                                          string          comment '可看房时间（其它）说明',
focus_point                                            string          comment '关注点',
resistance_point                                       string          comment '抗性点',
coordination_degree                                    int             comment '配合程度:1高，2中，3低',
has_main_push_projects                                 int             comment '客户感兴趣楼盘是否有当期主推楼盘1:否2:是',
order_status                                           int             comment '订单状态 0:非正常结束（半路中断，或没买房）10:未确认收到分配 20:已收到分配 30:已联系 40:已带看 45:排号 50:认购 55:草签 60:已签约',
investors                                              string          comment '出资人',
decisionor                                             string          comment '决策人',
disturbor                                              string          comment '干扰人',
family_funds                                           string          comment '家庭及资金',
close_explan                                           string          comment '订单关闭原因说明',
fun_project                                            int             comment '是否有商住需求:1是2否3不清楚',
is_pause_follow                                        int             comment '是否是暂停跟进关闭的订单 :0否1是',
order_pause_id                                         int             comment '订单暂停跟进表id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_require_history'
;
