drop table if exists julive_app.app_district_will_sale_project;
create table julive_app.app_district_will_sale_project(
district_id        string COMMENT '区县id', 
district_name      string COMMENT '区县名称', 
project_id         string COMMENT '楼盘id',
project_name       string COMMENT '楼盘名称',
city_id            string COMMENT '城市id',
city_name          string COMMENT '城市名称',
current_open_date  string COMMENT '楼盘开盘时间'
)
stored as parquet;
