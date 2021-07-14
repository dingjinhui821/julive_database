drop table if exists ods.cj_channel;
create external table ods.cj_channel(
channel_id                                             int             comment '',
channel_name                                           string          comment '',
weixin_service_pic                                     string          comment '',
weixin_subscribe_pic                                   string          comment '',
create_datetime                                        bigint          comment '',
update_datetime                                        bigint          comment '',
status                                                 int             comment '状态，1有效，0无效',
sort                                                   int             comment '排序字段',
phone_pc                                               string          comment '渠道400电话pc站',
phone_elite                                            string          comment '渠道400电话elite站',
phone_m                                                string          comment '渠道400电话m站',
phone_app                                              string          comment '渠道400电话app',
phone_bdsp                                             string          comment '百度小程序 400电话',
phone_esf_pc                                           string          comment '二手房pc 400 电话',
phone_esf_m                                            string          comment '二手房m 400 电话',
yiqixiu_url                                            string          comment '易企秀url',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
group_id                                               int             comment 'group id',
use_in_hotline                                         int             comment '注册来源（1:是  2:否)',
channel_type_id                                        int             comment '渠道分类id',
channel_type_name                                      string          comment '渠道分类名称',
account                                                string          comment '账户',
password                                               string          comment '密码',
token                                                  string          comment '权限代码',
channel_type                                           int             comment '渠道类型',
device_type                                            int             comment '设备类型',
app_type                                               int             comment 'app类型 1安卓 2苹果',
utm_source                                             string          comment '用来标记广告来源',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_channel'
;
