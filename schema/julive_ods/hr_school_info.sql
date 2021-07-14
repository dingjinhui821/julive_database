drop table if exists ods.hr_school_info;
create external table ods.hr_school_info(
id                                                     bigint          comment '自增id',
name                                                   string          comment '学校名称',
type                                                   int             comment '学校属性 1 985 ，2 211，3 一本，4 二本 ，5 三本，6 专科及以下 ，7 海外院校',
status                                                 int             comment '数据状态 1 正常 2 删除',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hr_school_info'
;
