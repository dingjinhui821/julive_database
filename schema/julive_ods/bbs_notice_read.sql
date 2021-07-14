drop table if exists ods.bbs_notice_read;
create external table ods.bbs_notice_read(
id                                                     int             comment '自增id',
forum_id                                               int             comment '帖子id',
employee_id                                            int             comment '员工id',
is_read                                                int             comment '是否已读 1 是 2 否',
read_datetime                                          int             comment '阅读时间',
type                                                   int             comment '类型 10:bug,20:建议,30:咨询 40:公告 50 消息',
content_type                                           int             comment '内容分类类型',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '0',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bbs_notice_read'
;
