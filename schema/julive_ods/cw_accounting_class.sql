drop table if exists ods.cw_accounting_class;
create external table ods.cw_accounting_class(
id                                                     int             comment '',
first_level_item_id                                    int             comment '会计一级科目id',
second_level_item_id                                   int             comment '会计二级科目id',
type                                                   int             comment '报销单类型 1、付款单、2差旅费报销单、3交通费报销单',
class_name                                             string          comment '费用类别名称',
is_all_city                                            int             comment '是否归属所有城市1、是 2 否',
is_all_department                                      int             comment '是否归属所有部门1.是 2 否',
first_level_sn                                         string          comment '一科科目编码',
second_level_sn                                        string          comment '二级科目编码',
fee_type                                               int             comment '费用类型 1、收入 2、支出',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cw_accounting_class'
;
