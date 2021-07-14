drop table if exists ods.yw_order;
create external table ods.yw_order(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
unite_order_id                                         bigint          comment '广义订单id',
channel_id                                             int             comment '对应渠道表的id',
source                                                 int             comment '订单了解途径',
user_id                                                bigint          comment '用户id',
user_realname                                          string          comment '用户姓名',
user_mobile                                            string          comment '用户手机号',
sex                                                    int             comment '用户性别1:男2:女',
employee_id                                            bigint          comment '员工id',
employee_realname                                      string          comment '员工姓名',
comment                                                string          comment '用户对侃家网的整体服务评价',
user_img                                               string          comment '用户照片',
status                                                 int             comment '-10:未处理 0:非正常结束（半路中断，或没买房）10:未确认收到分配 20:已收到分配 30:已联系 40:已带看 45:排号 50:认购 55:草签 60:网签',
see_employee_id                                        bigint          comment '带看咨询师id',
see_employee_realname                                  string          comment '带看咨询师姓名',
proxy_sale_id                                          bigint          comment '代理中介id',
proxy_sale_name                                        bigint          comment '代理中介名称',
is_distribute                                          int             comment '是否分配，及不分配的原因:1、分配 2、谈合作3、超区域4、不关注楼盘5、必须找售楼处6、关注二手房7、之前已上户8、不愿留电话9、电话无法接通99、其他',
is_import                                              int             comment '0不是通过历史数据导入的，1是通过历史数据导入的，2后来通过系统补录的',
broker_name                                            string          comment '经纪人姓名',
broker_phone                                           string          comment '经纪人电话',
distribute_datetime                                    int             comment '分配时间',
first_distribute_datetime                              int             comment '首次分配时间',
confirm_distribute_datetime                            int             comment 'ȷ',
delayed_contact_time                                   int             comment '延迟联系的时间:分配后首次联系的时间间隔',
delayed_see_project_time                               int             comment '首次联系到首次带看的时间间隔',
note                                                   string          comment '',
re_distribute_datetime                                 int             comment '',
history_employee_id                                    string          comment '历史咨询师',
history_employee_realname                              string          comment '历史咨询师姓名',
device_type                                            int             comment '上户设备类型:0未知，1pc，2移动',
follow_service                                         string          comment '跟进客服',
user_cookie                                            string          comment '用户的cookie',
repeat_order                                           bigint          comment '重复订单号',
is_cancel_appointment                                  int             comment '是否取消预约联系或带看，0继续跟进，1取消',
clue_cooperation_status                                int             comment '订单获得线索时是否是合作订单，根据订单生成时，订单中的推荐、感兴趣楼盘是否包含有合作楼盘来判断，有1，无2，默认空',
abtest_cookie_all                                      string          comment '此用户所有abtest的cookie值，多个以|分隔',
incustomer_cooperation_status                          int             comment '订单分配（上户）时是否是合作订单，根据订单生成时，订单中的推荐、感兴趣楼盘是否包含有合作楼盘来判断，有1，无2，默认空',
public_pool_flag                                       int             comment '客户是否放入公共池1是2否',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
recommender_id                                         bigint          comment '友介老客户id',
type                                                   int             comment '订单类型1:后台订单2:前台订单',
improve_house_flag                                     int             comment '是否改善性住房:1是，2否',
source_order_id                                        bigint          comment '来源订单id',
more_mobile                                            string          comment '备用联系方式',
re_incustomer                                          int             comment '客户再上户购房可能性:1:高2:中3:低',
is_batch_alloc                                         int             comment '是否批量分配:1，否，2，是',
clue_quality                                           int             comment '线索质量 1:优质，2:非优质',
op_type                                                int             comment 'op type',
distribute_category                                    int             comment '一级分类名称',
maybe_customer                                         int             comment '再上户可能性',
is_short_alloc                                         int             comment '是否符合路径缩短计划',
qa_employee_id                                         int             comment 'pc/m/app用户向某个咨询师提问的咨询师id',
intention_employee_id                                  int             comment '意向咨询师',
is_check_project                                       int             comment '新订单是否生成线索质量分析，1是，2否',
alloc_to_server                                        int             comment '订单分配给客服0:否 1:是 区分creator 客服id或用户id',
server_distribute_datetime                             int             comment '客服分配时间',
recommender_order_id                                   bigint          comment '友介订单id',
sys_tag                                                int             comment '0:无优先级标签 10:c 20:b 30:a 40:头部客户',
alloc_type                                             int             comment '分配类型 10批量分配 20溢出分配 30指定分配 40特殊线索分配',
hope_contact_time                                      int             comment '客户期望联系时间0:正常联系,具体时间时间戳:稍后联系',
re_incustomer_type                                     int             comment '重上户类型:1普通重上户 2无历史重上户',
called_num                                             int             comment '客服已呼叫次数',
last_call_time                                         int             comment '上次呼叫时间',
can_call_status                                        int             comment '呼叫状态（1:可呼叫，0:不可呼叫，2:审批可呼叫）',
before_last_call_time                                  int             comment '留存上上一次开始通话时间',
short_alloc_obj_type                                   int             comment '分配对象:2按排名5:按邀约率',
alloc_obj_val                                          int             comment '分配对象数值',
short_alloc_type                                       int             comment 'a组:1b组:2',
short_pop                                              int             comment '0:未弹出1:已弹出',
alloc_policy                                           int             comment '分配策略 1:a组 2:b组',
order_alloc_type                                       int             comment '订单分配成功是所属分配类型:100:路径缩短-指定问答分配 101:神秘客户 102:路径缩短-特殊线索分配 103:路径缩短-rank分配 104:路径缩短-avg分配 105:客服创建订单指定分配 106:特殊线索分配 107:rank分配 108:avg分配',
is_confirm                                             int             comment '是否确认 1:确认 2:放弃',
has_see_julive_ad                                      int             comment '是否看过居理品牌广告,1 是、2 否、3:未回答 默认为0',
care_house                                             string          comment '1: 未关注10:已关注-二手房 11:二级不分配原因为关注二手房',
sobot_service_id                                       string          comment '智齿客服id',
customer_intent_city                                   bigint          comment '客户意向城市',
guide_to_developer                                     int             comment '是否将该订单引导到开发商订单 0:未知1:引导到开发商订单2:不引导开发商订单',
care_developer                                         int             comment '是否生成开发商订单，1是，2否，默认否',
order_recommender                                      int             comment '订单推荐方 0:未知1、自营咨询师2、开发商',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order'
;