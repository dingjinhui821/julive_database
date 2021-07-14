
--jjulive_app.app_market_offline_conversion
drop table if exists julive_app.app_market_offline_conversion_market;
create table if not exists julive_app.app_market_offline_conversion_market(
report_date            string          comment '业务日期yyyy-MM-dd',  
city_id                int             comment '业务城市id',       
channel_id             int             comment '渠道id',     
media_type             string          comment '媒体类型名称',      
is_short               string          comment '是否缩短路径', 
org_name                     string       comment '公司名称',    
full_type                    string       comment '所属咨询师是否转正',                                                                   
yw_line                      string       comment '业务线',
rengou_yingshou        decimal(19,4)   comment '认购-含退、含外联收入',  
rengou_yingshou_net    decimal(19,4)   comment '认购(不含退)-含外联佣金', 
qianyue_yingshou       decimal(19,4)   comment '原合同预测总收入',
xs_cnt                 int             comment '线索量',
sh_cnt                 int             comment '上户量',
dk_cnt                 int             comment '带看量',
rg_cnt                 int             comment '认购量',
qy_cnt                 int             comment '签约量',
probs                  decimal(19,4)   comment '预测的上户质量分',
`400_xs_cnt`           int             comment '400线索量',
developer_xs_cnt       int             comment '开发商线索量',
jietong_sh_day         int             comment '通话次数',
xs_score                 decimal(19,4) comment '线索质量分',
first_call_duration      int           comment '首次通话时长',
first_call_duration_num  int           comment '首次通话时数量',
intent_low_num           int           comment '转为无意向数量(无意向时间-分配时间>0)',
xs_cnt_all               int           comment  '线索总量(包含yw_order_kfs)',
online_dk_cnt            int           comment  '线上带看量', 
rg_cnt_net               int           comment  '认购量(不含退)-含外联',
`400_sh_cnt`            int             comment '400线索量'
) comment 'APP-市场线下数据表' 
stored as parquet 
;
