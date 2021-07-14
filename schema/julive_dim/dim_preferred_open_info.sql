drop table if exists julive_dim.dim_preferred_open_info;
create table julive_dim.dim_preferred_open_info(
city_id       string comment '城市ID',
equipment_id  string comment '端id',
is_open       string comment '设备(1.居理新房pc站2.居理新房m站3.客服4.支撑系统10.二手房pc站,20.二手房m站,101.居理新房android,201.居理新房ios,102.居理咨询师android,301.居理新房小程序,302.居理新房小程序（城市版）,303.居理新房小程序（楼盘版）401.百度小程序)',
start_date    string comment '开始时间',
end_date      string comment '结束时间',
etl_time      string comment '跑数时间'
)
comment'优选开关时间状态表'
stored as parquet;
