drop table if exists ods.bbs_landlord_reply_record;
create external table ods.bbs_landlord_reply_record(
id                                                     int             comment '自增id',
forum_id                                               int             comment '帖子id',
inviter_id                                             int             comment '邀请人id',
is_answer                                              int             comment '是否回答 1 是 2 否',
invites_id                                             int             comment '被邀请人id',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bbs_landlord_reply_record'
;
