drop table if exists ods.yw_user_feedback;
create external table ods.yw_user_feedback(
id                                                     bigint          comment '',
create_datetime                                        int             comment '创建时间',
creator                                                bigint          comment '创建人(用户id)',
content                                                string          comment '评论内容',
mobile                                                 string          comment '电话',
install_id                                             int             comment '设备id app记录设备id',
user_cookie                                            string          comment '用户cookie pc记录cookie',
status                                                 int             comment '状态 1未处理 2已处理 3已删除',
obj_id                                                 int             comment '对象id(订单/楼盘等)',
type                                                   int             comment '类型(0无类型/1订单/2楼盘等)',
source                                                 int             comment '来源(我的/行程/服务)',
product_id                                             int             comment 'pc=1，m=2，app android=101，app ios=201',
channel_id                                             int             comment '渠道id',
city_id                                                int             comment '城市id',
order_id                                               string          comment '订单id',
employee_id                                            string          comment '订单所属咨询师id',
order_status                                           string          comment '订单状态',
contact_information                                    string          comment '联系方式（手机号，qq号，邮箱等）',
feedback_type                                          int             comment '0:电话骚扰,1:取消预约,2:信息泄露,3:客服服务差,4:咨询师服务差,5:其他',
employee_realname                                      string          comment '所属咨询师姓名',
op_type                                                int             comment '留电位置',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_user_feedback'
;
