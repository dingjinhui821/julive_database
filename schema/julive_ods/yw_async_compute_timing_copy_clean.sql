drop table if exists julive_bak.yw_async_compute_timing_copy_clean;
create external table julive_bak.yw_async_compute_timing_copy_clean(
id                                                     bigint          comment '主键id',
`key`                                                  string          comment '关键字',
class_obj                                              string          comment '序列化后的类对象',
class_name                                             string          comment '类名字',
param_array                                            string          comment '参数数组',
class_dir                                              string          comment '类所在路径',
do_datetime                                            int             comment '执行时间',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
status                                                 int             comment '监控执行状态:-1删除0未执行1执行成功2执行失败',
key_md5                                                string          comment 'key值的md5加密值',
async_random                                           int             comment '执行随机数，默认值1，系统会按照此随机数来分成多个脚本执行',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/julive_bak/yw_async_compute_timing_copy_clean'
;
