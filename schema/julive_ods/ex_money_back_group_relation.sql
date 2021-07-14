drop table if exists ods.ex_money_back_group_relation;
create external table ods.ex_money_back_group_relation(
id                                                     int             comment 'id',
group_id                                               int             comment '回款组id',
deal_id                                                int             comment '成交单id',
commission_type                                        int             comment '佣金类型 1前置电商 2 后置返费 3 成交奖',
return_step                                            int             comment '返费阶段',
step_num                                               int             comment '佣金类型+阶段相同的第几笔回款',
is_from_jump_contract                                  int             comment '是否是跳点合同生成的',
follow_money                                           double          comment '跟进金额',
contract_id                                            int             comment '合同id',
contract_category_id                                   int             comment '合同分类id',
delete_status                                          int             comment '删除状态 1是2否',
move_status                                            int             comment '移除状态 1是2否',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
delete_cause                                           int             comment '删除原因1.合同预测变化',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_money_back_group_relation'
;
