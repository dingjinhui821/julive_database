drop table if exists julive_app.app_market_meiti_channel_zhuanhua_time;
create table if not exists julive_app.app_market_meiti_channel_zhuanhua_time( 
pdate                                    string            COMMENT '事件日期',   
channel_id                               string            COMMENT '渠道ID',   
channel_name                             string            COMMENT '渠道名称',   
channel_type_name                        string            COMMENT '渠道分类名称',  
select_city                              string            COMMENT '城市ID',
city_name                                string            COMMENT '城市名称',
view_pv                                  bigint            COMMENT 'PV',
view_uv                                  bigint            COMMENT 'UV',   
400_pv                                   bigint            COMMENT '400PV', 
400_uv                                   bigint            COMMENT '400UV',
xs_cnt                                   bigint            COMMENT '线索数',    
sh_cnt                                   bigint            COMMENT '上户数',
dk_cnt                                   bigint            COMMENT '带看数',
rg_cnt_contains_cancel                   bigint            COMMENT '认购数含退',
rg_cnt_notcontains_cancel                bigint            COMMENT '认购数不含退',
income_contains_cancel                   decimal(29,4)     COMMENT '认购金额含退',
income_notcontains_cancel                decimal(29,4)     COMMENT '认购金额不含退')
COMMENT '商务渠道转化指标时间维度'
stored as parquet;


 

