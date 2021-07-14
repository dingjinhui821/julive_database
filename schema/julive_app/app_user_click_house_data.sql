drop table if exists julive_app.app_user_click_house_data;
create table julive_app.app_user_click_house_data(
global_id           string comment '全局用户标识',
comjia_unique_id    string comment '用户ID',
julive_id           string comment '居理用户ID',
user_id             string comment '神策USER_ID',
product_id          string comment '端id',
house_id            string comment '户型id',
create_time         string comment '创建时间',
etl_time            string comment '跑数时间'
)
PARTITIONED BY (pdate string comment '分区日期')

stored as parquet;

