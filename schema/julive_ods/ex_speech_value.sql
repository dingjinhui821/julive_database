drop table if exists ods.ex_speech_value;
create external table ods.ex_speech_value(
id                                                     int             comment '',
title                                                  string          comment '价值点标题',
content                                                string          comment '话术内容',
status                                                 int             comment '状态 -1删除 1启用 2禁用',
type                                                   int             comment '类型 1系统话术 2模拟邀约”其他“ 3预约联系”其他“',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_speech_value'
;
