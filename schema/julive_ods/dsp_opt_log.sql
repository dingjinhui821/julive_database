drop table if exists ods.dsp_opt_log;
create external table ods.dsp_opt_log(
id                                                     int             comment '',
product_type                                           int             comment '产品形态，1:feed，2:导航,3:app渠道,4:sem,5:免费,6:其他,7:端口,8:小程序',
account_name                                           string          comment '账户名',
dsp_account_id                                         int             comment '账户id',
media_type                                             int             comment '渠道分类',
plan_id                                                bigint          comment '计划id',
plan_name                                              string          comment '计划名称',
unit_id                                                bigint          comment '单元id',
unit_name                                              string          comment '单元名称',
creative                                               string          comment '创意',
keyword                                                string          comment '关键词',
opt_content                                            string          comment '操作内容',
opt_content_desc                                       string          comment '操作内容描述',
opt_type                                               int             comment '操作类型 1:设置 2:新增 3:删除 4:修改 5:暂停 6:启用 7:重命名 8:激活 9:取消 10:系统激活 11:关键词转移',
opt_level                                              int             comment '操作层级 1:推广单元 2:推广计划 3:帐户 4:创意 5:关键词 7:蹊径子链 8:app 推广 9:推广电话 10:商桥 11:页面回呼 12:动态创意',
old_value                                              string          comment '操作前内容',
new_value                                              string          comment '操作后内容',
opt_obj                                                string          comment '被操作对象名称',
opt_time                                               int             comment '操作时间戳',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_opt_log'
;
