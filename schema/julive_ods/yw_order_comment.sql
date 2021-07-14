drop table if exists ods.yw_order_comment;
create external table ods.yw_order_comment(
id                                                     bigint          comment '主键id',
order_id                                               bigint          comment '订单id',
comment                                                string          comment '评论内容',
comment_user                                           string          comment '用户评论内容',
user_img                                               string          comment '用户成交图片',
user_id                                                bigint          comment '用户id',
user_name                                              string          comment '用户展示名字',
user_mobile                                            string          comment '用户手机号码',
project_id                                             bigint          comment '成交楼盘id',
project_name                                           string          comment '成交楼盘名字',
employee_id                                            bigint          comment '负责咨询师id',
employee_name                                          string          comment '负责咨询师姓名',
trade_datetime                                         int             comment '成交时间',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
status                                                 int             comment '状态:10未评价，20待审核，30合格，40不合格',
marvellous                                             int             comment '加精 0取消 1加精',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
is_authorized                                          int             comment '用户是否同意:0未同意  1已同意',
comment_datetime                                       int             comment '评价时间',
publish_status                                         int             comment '1已发布，2未发布',
audit_unqualified_type                                 int             comment '审核不合格类型',
audit_unqualified_reason                               string          comment '审核不合格原因',
new_logic                                              int             comment '邀评新逻辑:1京东购物卡，2否，3家财险,4用户选择',
is_sync                                                int             comment '标记是否已经同步过数据（1:没有同步过，2:已经同步，3:数据异常，如order_id为空、yw_order表上户(first_distribute_datetime、distribute_datetime)时间都为空）',
taxi_price                                             int             comment '节约路费',
see_project_num                                        int             comment '看过楼盘个数',
period_day                                             int             comment '买房历时',
start_datetime                                         int             comment '上户时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_comment'
;
