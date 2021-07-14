drop table if exists julive_app.app_esf_house_strict_selection;
create table julive_app.app_esf_house_strict_selection(
district_id        string COMMENT '区县id', 
district_name      string COMMENT '区县名称', 
house_id           string COMMENT '二手房房源id',
vallage_name       string COMMENT '二手房房源所在小区名称',
total_price        string COMMENT '二手房房源价格',
city_id            string COMMENT '城市id',
city_name          string COMMENT '城市名称'
)
stored as parquet;
