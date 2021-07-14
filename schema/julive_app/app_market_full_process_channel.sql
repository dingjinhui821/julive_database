CREATE TABLE if not exists julive_app.app_market_full_process_channel(
  `report_date`              string         comment '业务日期yyyy-MM-dd', 
  `city`                     string         comment '城市名称', 
  `device_type`              string         comment '设备名称', 
  `media_type`               string         comment '媒体类型名称',  
  `product_type`             string         comment '模块名称', 
  `app_type`                 string         comment 'APP类型 android ios',
  `channel_id`               int            comment '渠道id',
  `channel_name`             string         comment '渠道名称', 
  `show_num`                 int            comment '展示量', 
  `click_num`                int            comment '点击量', 
  `bill_cost`                double         comment '账单消耗', 
  `cost`                     double         comment '实际消耗', 
  `xs_cnt`                   int            comment '线索量',
  `sh_cnt`                   int            comment '上户量',
  `dk_cnt`                   int            comment '带看量',
  `rg_cnt`                   int            comment '认购量',
  `qy_cnt`                   int            comment '签约量',
  `rengou_yingshou`          double         comment '认购-含退、含外联收入', 
  `rengou_yingshou_net`      double       comment '认购(不含退)-含外联佣金', 
  `qianyue_yingshou`         double       comment '原合同预测总收入',
  `probs`                    double       comment '预测的上户质量分', 
  `city_group`               string       comment '不用', 
  `zhufucity_dabao`          string       comment '不用', 
  `region`                   string       comment '所属大区', 
  `city_type`                string       comment '城市类型 老城（含副区） 新城',             
  `400_xs_cnt`               bigint       comment '400线索量',               
  `developer_xs_cnt`         bigint       comment '开发商线索量',               
  `jietong_sh_day`           bigint       comment '通话次数',                 
  `xs_score`                 double       comment '线索质量分',                
  `first_call_duration`      double       comment '首次通话时长',               
  `intent_low_num`           bigint       comment '首次通话时数量',              
  `xs_cnt_all`               bigint       comment '转为无意向数量(无意向时间-分配时间>0)',
  `first_call_duration_num`  bigint       comment '线索总量(包含yw_order_kfs)',
  `online_dk_cnt`            bigint       comment '线上带看量', 
  `rg_cnt_net`               bigint       comment '认购量(不含退)-含外联',
  `400_sh_cnt`               bigint       comment '400上户量', 
  `yw_line`                  string       comment '业务线'
) comment 'APP-市场全流程渠道城市数据表' 
stored as parquet 
;



