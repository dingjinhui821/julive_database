drop table if exists ods.yw_user_project_comment;
create external table ods.yw_user_project_comment(
id                                                     bigint          comment '',
create_datetime                                        int             comment '用户创建时间',
creator                                                bigint          comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                bigint          comment '更新人',
user_id                                                bigint          comment '用户id',
user_status                                            int             comment '用户状态 1业主 2看过此楼盘',
project_id                                             bigint          comment 'comjia新生成楼盘id',
content                                                string          comment '评论内容',
img_count                                              int             comment '图片总数',
user_name                                              string          comment '用户名称',
comment_rat                                            double          comment '评分',
install_id                                             int             comment '设备id app记录设备id',
user_cookie                                            string          comment '用户cookie pc记录cookie',
status                                                 int             comment '状态  1待审核 2审核通过 3审核拒绝 4删除',
like_num                                               bigint          comment '赞总数',
nickname                                               string          comment '',
source                                                 int             comment '来源(1.用户实际点评 2.后台导入)',
city_id                                                int             comment '城市id',
audit_time                                             int             comment '审核时间（通过或驳回时间）',
operator                                               int             comment '审批通过/驳回人',
backend_create_datetime                                int             comment '后台系统创建时间',
is_repeat                                              int             comment '是否重复 1重复 2不重复',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_user_project_comment'
;
