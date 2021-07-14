drop table if exists ods.ex_contract_cate_condition;
create external table ods.ex_contract_cate_condition(
id                                                     bigint          comment 'id',
contract_id                                            int             comment '结佣合同id',
cate_condition                                         int             comment '分类条件:1楼栋类型 2楼栋号码 3户型类型 4户型居室 5面积 6客户付款方式 7认购转签约时长 8认购转首付款交齐时长 9首付比例,10价格,11楼层,12朝向,13装修情况,14楼栋位置,15团购费',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_contract_cate_condition'
;
