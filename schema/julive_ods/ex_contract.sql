drop table if exists ods.ex_contract;
create external table ods.ex_contract(
id                                                     int             comment '合同id',
contract_number                                        string          comment '合同编号',
contract_begin_datetime                                bigint          comment '合同开始时间',
contract_end_datetime                                  bigint          comment '合同结束时间',
deal_step                                              int             comment '成交确认关键节点，10排号 30认购 50草签 70网签',
have_diff_cate                                         int             comment '结佣条款是否基于不同条件有不同分类 1有 2无',
cate_condition                                         string          comment '分类条件:1楼栋类型 2楼栋号码 3户型类型 4户型居室 5面积 6客户付款方式 7认购转签约时长 8认购转首付款交齐时长 9首付比例',
have_jump_maids                                        int             comment '是否有跳佣/跳点，1有，2无',
jump_rule                                              string          comment '跳佣/点规则',
have_other_rule                                        int             comment '是否有其他特殊规则 1有 2无',
other_rule                                             string          comment '其他特殊规则',
project_id                                             bigint          comment '楼盘id',
creator                                                bigint          comment '创建人',
create_datetime                                        bigint          comment '创建时间',
updator                                                bigint          comment '更新人',
update_datetime                                        bigint          comment '更新时间',
arrival_datetime                                       bigint          comment '合同到达时间',
is_delete                                              int             comment '0:未删除 1:已删除',
payback_employee_id                                    string          comment '回款负责人,逗号分隔',
city_id                                                int             comment '城市id',
is_head_project                                        int             comment '是否是头部楼盘 1头部楼盘 2腰部楼盘 3尾部楼盘',
ex_sign_id                                             int             comment '签约单id',
ex_order_id                                            int             comment 'bd单id',
cooperate_type                                         int             comment '合作方式 1开发商直签 2平台合作 3战略合作',
partner_id                                             string          comment '合作方公司id,逗号分隔',
price_is_same                                          int             comment '居理与自访客户购买价格是否一致1.是2.否',
is_complete_talk                                       int             comment '是否对合同内容及履约情况完成前置沟通 1.是 2.否',
note                                                   string          comment '合同备注信息',
platform_commission                                    double          comment '平台抽佣点位',
julive_expect_commission                               double          comment '居里预期抽成点位',
commission_is_conflict                                 int             comment '案场是否有规则（如老带新）会导致客户购房价格和渠道佣金冲突 1是 2否',
is_have_exclusion_item                                 int             comment '是否有排他条款1.是2.否',
is_complete_talk_approval_status                       int             comment '前置沟通审核状态1待上传、2待审核、3不合格、4合格,6老数据',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_contract'
;
