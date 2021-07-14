drop table if exists julive_dim.dim_city_history;
CREATE TABLE julive_dim.dim_city_history(
skey               string COMMENT '主键', 
city_id            bigint COMMENT '源系统城市ID', 
city_name          string COMMENT '城市名称', 
city_seq           string COMMENT '开城顺序', 
region             string COMMENT '所属大区', 
bd_region          string COMMENT '渠道大区',
city_type          string COMMENT '城市类型 老城（含副区） 新城', 
city_type_first    string COMMENT '城市类型1 老城（含副区） 新城', 
city_type_second   string COMMENT '城市类型2 12老城 副区', 
city_type_third    string COMMENT '城市类型3 11老城 13新城 副区', 
mgr_city_seq       string COMMENT '主城 开城顺序城市名称', 
mgr_city_id        string COMMENT '主城id', 
mgr_city           string COMMENT '主城名称', 
deputy_city        string COMMENT '主副城',
show               string comment 'c端展示，1-展示，2-未展示', 
lng                string COMMENT '城市坐标经度', 
lat                string COMMENT '城市坐标纬度'
)
partitioned by (pdate string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'LINES TERMINATED BY '\n'STORED AS TEXTFILE;