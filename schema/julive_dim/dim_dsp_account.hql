drop table if exists julive_dim.dim_dsp_account;
create table if not exists julive_dim.dim_dsp_account( 
id                       int            COMMENT '账户ID',   
media_type               int            COMMENT '媒体类型',   
account                  string         COMMENT '账户',   
param_id                 int            COMMENT '参数表id',  
params                   string         COMMENT '请求api时需要传递的参数(json格式)',
status                   string         COMMENT '状态（1:启用2:停用）',
creator                  bigint         COMMENT '创建者',
updator                  bigint         COMMENT '更新者',   
product_type2            int            COMMENT '投放类型（备用，包含app）', 
app_type                 string         COMMENT 'app类型，0非app 1 ios 2 安卓 3 小程序',
media_type_name          string         COMMENT '媒体类型名称',    
product_type_name        string         COMMENT '产品形态名称',
city                     string         COMMENT '城市名称',
create_time              string         COMMENT '创建时间',
agent                    string         COMMENT '代理或者负责人',
etl_time                 STRING         COMMENT 'ETL跑数时间')
COMMENT 'DSP账户全量表'
stored as parquet;




drop table if exists julive_dim.dim_dsp_account_history;
create table if not exists julive_dim.dim_dsp_account_history( 
id                       int            COMMENT '账户ID',   
media_type               int            COMMENT '媒体类型',   
account                  string         COMMENT '账户',   
param_id                 int            COMMENT '参数表id',  
params                   string         COMMENT '请求api时需要传递的参数(json格式)',
status                   string         COMMENT '状态（1:启用2:停用）',
creator                  bigint         COMMENT '创建者',
updator                  bigint         COMMENT '更新者',   
product_type2            int            COMMENT '投放类型（备用，包含app）', 
app_type                 string         COMMENT 'app类型，0非app 1 ios 2 安卓 3 小程序',
media_type_name          string         COMMENT '媒体类型名称',    
product_type_name        string         COMMENT '产品形态名称',
city                     string         COMMENT '城市名称',
create_time              string         COMMENT '创建时间',
agent                    string         COMMENT '代理或者负责人',
p_date                   string         COMMENT '快照时间')
COMMENT 'DSP账户快照表'
PARTITIONED BY (pdate STRING)
stored as parquet;














CREATE TABLE julive_dim.dim_dsp_account_history (   id INT COMMENT '账户ID',   media_type INT COMMENT '媒体类型',   account STRING COMMENT '账户',   param_id INT COMMENT '参数表id',   params STRING COMMENT '请求api时需要传递的参数(json格式)',   status STRING COMMENT '状态（1:启用2:停用）',   creator BIGINT COMMENT '创建者',   updator BIGINT COMMENT '更新者',   product_type2 INT COMMENT '投放类型（备用，包含app）',   app_type STRING COMMENT 'app类型，0非app 1 ios 2 安卓 3 小程序',   media_type_name STRING COMMENT '媒体类型名称',   product_type_name STRING COMMENT '产品形态名称',   city STRING COMMENT '城市名称',   create_time STRING COMMENT '创建时间',   agent STRING COMMENT '代理或者负责人',   p_date STRING COMMENT '快照时间' ) PARTITIONED BY (   pdate STRING )  COMMENT 'DSP账户快照表'