drop table if exists julive_fact.fact_android_app_store_cost_indi;
create table if not exists julive_fact.fact_android_app_store_cost_indi( 
report_date                  STRING             COMMENT '日期字符串:yyyy-MM-dd',
account_id                   STRING             COMMENT '账户ID',
account                      STRING             COMMENT '账户名称',
agent                        STRING             COMMENT '代理或者负责人',
plan_id                      STRING             COMMENT '计划ID',
plan_name                    STRING             COMMENT '计划名称',
media_type_name              STRING             COMMENT '媒体类型名称',
app_type                     STRING             COMMENT 'APP类型',
show_num                     BIGINT             COMMENT '展示',
click_num                    BIGINT             COMMENT '点击',
bill_cost                    DECIMAL(19,4)      COMMENT '消耗',
cost                         DECIMAL(19,4)      COMMENT '真实消耗',
download_num                 BIGINT             COMMENT '下载量',
etl_time                     STRING             COMMENT 'ETL跑数时间')
COMMENT '安卓应用市场展点消明细'
stored as parquet;