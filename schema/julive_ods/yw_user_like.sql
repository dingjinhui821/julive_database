drop table if exists ods.yw_user_like;
create external table ods.yw_user_like(
id                                                     int             comment '',
type                                                   int             comment '1.用户点评楼盘点赞，2信息流，3文章,4咨询师点评,5对咨询师点赞 6答案 8 视频点赞',
obj_id                                                 bigint          comment '点赞对象的id',
user_id                                                bigint          comment '登陆用户id',
install_id                                             int             comment '设备id app记录设备id',
user_cookie                                            string          comment '用户cookie pc记录cookie',
status                                                 int             comment '状态 1已赞 2取消赞',
create_datetime                                        int             comment '创建时间',
creator                                                bigint          comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                bigint          comment '更新人',
user_type                                              int             comment '用户类型，1用户，2员工',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_user_like'
;
