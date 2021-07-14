drop table if exists ods.common_op_type;
create external table ods.common_op_type(
id                                                     int             comment 'op_type 的值',
op_type_name                                           string          comment 'op_type 的英文名称',
description                                            string          comment '描述',
notice                                                 string          comment '设置成不可留电时，提示改文案',
site                                                   int             comment '0: 无  1: pc  2: m  3: pc_m  4: app  5: other',
from_page                                              string          comment '所在页面',
from_module                                            string          comment '所属模块',
status                                                 int             comment '0:删除1:正常',
is_leave_mobile                                        int             comment '是否留电 1是 2否',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/common_op_type'
;
