drop table if exists julive_dim.dict_transcoding;
create external table julive_dim.dict_transcoding(
id                                                     int             comment '',
code_type                                              string          comment '码类型',
code_desc                                              string          comment '码中文描述',
from_tab_col                                           string          comment '来源业务表字段,完整格式,可适当选择描述:[db.table.column]',
src_code_value                                         string          comment '源码值',
src_code_name                                          string          comment '源描述',
tgt_code_value                                         string          comment '目标码值',
tgt_code_name                                          string          comment '目标描述',
is_general                                             int             comment '是否为通用码:1 是 0 否',
is_repeat                                              int             comment '是否已重复:1是0否',
create_time                                            string          comment '创建时间',
update_time                                            string          comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dict_transcoding'
;
