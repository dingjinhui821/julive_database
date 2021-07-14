drop table if exists ods.cw_accounting_items;
create external table ods.cw_accounting_items(
id                                                     int             comment '',
item_name                                              string          comment '科目名称',
type                                                   int             comment '科目类别 1.一级 2 二级',
use_count                                              int             comment '使用频次',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cw_accounting_items'
;
