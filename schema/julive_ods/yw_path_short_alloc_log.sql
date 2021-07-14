drop table if exists ods.yw_path_short_alloc_log;
create external table ods.yw_path_short_alloc_log(
id                                                     int             comment '',
order_id                                               bigint          comment '订单id',
rule_id                                                int             comment '路径缩短配置规则id',
do_datetime                                            int             comment '分配日期',
city_id                                                int             comment '城市id',
employee_id                                            int             comment '员工id',
type                                                   int             comment 'a组:1 or b组:2',
alloc_rule                                             string          comment '路径缩短分配的规则',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
rule_type                                              int             comment '分配类型（1-路径缩短 2.优选客户）',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_path_short_alloc_log'
;
