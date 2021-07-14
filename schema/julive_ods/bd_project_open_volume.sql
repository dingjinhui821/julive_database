drop table if exists ods.bd_project_open_volume;
create external table ods.bd_project_open_volume(
id                                                     bigint          comment 'id',
project_id                                             int             comment '楼盘id',
building_id                                            int             comment '楼栋id',
type                                                   int             comment '字段类型 1.交房时间， 2.放量时间',
value_type                                             int             comment '字段值类型 字段类型为1（1预计交房时间 2实际交房时间），字段类型为2 （1.已开盘 3.预计开盘）',
date_year                                              int             comment '交房/放量年份',
date_month                                             int             comment '交房/放量时间月份',
date_day                                               int             comment '交房/放量时间日',
date_ten                                               int             comment '交房/放量时间旬 1上旬 2中旬 3下旬',
system_reminder_time                                   int             comment '系统提醒时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
parent_open_volume_id                                  int             comment '父放量或交房记录id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_project_open_volume'
;
