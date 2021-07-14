


-- FACT-注册事件日志

drop table if exists julive_fact.fact_event_sign_up_base_dtl;
create EXTERNAL table if not exists julive_fact.fact_event_sign_up_base_dtl(
global_id                             string          comment 'global_id',
create_datetime                       int             comment '事件发生时间',
julive_id                             int             comment 'julive id',
channel_id                            int             comment '渠道ID',
channel_put                           string,
app_id                                int             comment '101安卓 201苹果 301微信小程序 401百度小程序',
product_id                            string          comment '产品ID 1PC 2M 101安卓 201苹果',
select_city                           int             comment '选择城市ID',
utm_source                            string          comment '用来标记广告来源',
event                                 string          comment '事件名称',
os                                    string          comment '操作系统',
os_version                            string          comment '操作系统版本',
channel                               string          comment '渠道名',
`$city`                               string          comment 'IP解析城市名',
install_datetime                      string             comment '激活时间',
aid                                   bigint          comment '广告ID',
cid                                   bigint          comment '创意ID',
etl_time                              string          comment '任务执行时间：yyyy-MM-dd HH:mm:ss'
) comment 'FACT-注册事件日志' 
partitioned by (pdate string comment '业务发生日期')
stored as parquet 
;
 