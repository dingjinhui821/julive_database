set hive.execution.engine=spark;
set spark.app.name=dim_dsp_account;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;

--   功能描述：DSP账户维度表
--   输 入 表：
--            ods.dsp_account                        -- DSP账户表
--            julive_dim.dim_city                    -- 城市维度表


--   输 出 表：julive_dim.dim_dsp_account
-- 
--   创 建 者：  杨立斌  18612857267
--   创建日期： 2021/05/18 15:41
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期： 修改人   修改内容
--  20210625   jiangbaoqiao  修改微信公众号为微信

insert overwrite table julive_dim.dim_dsp_account
SELECT
        dsp_account.id,
        media_type,
        account,
        param_id,
        params,
        if(status=1,'启用',if(status=2,'停用',null)) as status,

        creator,
        updator,
        product_type2,
        if(app_type = 3,'小程序',
        if(app_type=2,'安卓',
        if(app_type=1,'苹果',null))) as app_type,

        if(media_type = 1 and product_type2 = 3 and product_type!=4,'百度信息流',
        if(media_type = 1 and (product_type = 4 or product_type = 1),'百度',
        if(media_type = 2 and product_type2 = 3,'360信息流',
        if(media_type = 2,'360',
        if(media_type = 3,'搜狗',
        if(media_type = 4,'今日头条',
        if(media_type = 5,'广点通', --20210428 腾讯智慧推 修改成了广点通
        if(media_type = 8,'其他',
        if(media_type = 10,'导航',
        if(media_type = 11 and product_type2 = 4,'神马',
        if(media_type = 11 and product_type2 != 4,'UC',
        if(media_type = 12,'厂商',
        if(media_type = 13,'微信',
        if(media_type = 14,'端口',
        if(media_type = 15,'广点通',
        if(media_type = 16,'站内导流',
        if(media_type = 17 or media_type = 36 or media_type = 37 or media_type = 38 or media_type = 39,'应用市场（安卓）',
        if(media_type = 18,'应用市场（苹果）',
        if(media_type = 20,'爱奇艺',
        if(media_type = 21,'网易新闻',
        if(media_type = 22,'网易有道',
        if(media_type = 23,'快手',
        if(media_type = 24,'知乎',
	      if(media_type = 19,'UC',
        if(media_type = 25,'微信',-- 20210625 修改微信公众号为微信
        if(media_type = 26,'微博粉丝通',null)))))))))))))))))))))))))) as media_type_name,

        if(product_type2 = 1,'feed',
	      if(product_type2 = 3 and app_type = 3,'小程序',
        if(product_type2 = 3,'APP渠道',
        if(product_type2 = 4,'SEM',null)))) as product_type_name,

        d1.city_name as city,
        from_unixtime(create_datetime) as create_time,
	      agent,
        current_timestamp() as etl_time

from ods.dsp_account

left join julive_dim.dim_city d1
on dsp_account.city_id = d1.city_id;



insert overwrite table julive_dim.dim_dsp_account_history
partition (pdate)

SELECT
        id,
        media_type,
        account,
        param_id,
        params,
        status,
        creator,
        updator,
        product_type2,
        app_type,
        media_type_name,
        product_type_name,
        city,
        create_time,
        agent,
        date_add(current_date(),-1) as p_date,
        FROM_UNIXTIME(UNIX_TIMESTAMP(date_add(current_date(),-1),'yyyy-MM-dd'),'yyyyMMdd') as pdate
FROM julive_dim.dim_dsp_account;


CREATE VIEW IF NOT EXISTS db_l.deal_dsp_account (
id                                                  COMMENT '账户ID', 
media_type                                          COMMENT '媒体类型', 
account                                             COMMENT '账户',  
param_id                                            COMMENT '参数表id', 
params                                              COMMENT '请求api时需要传递的参数',
status                                              COMMENT '状态（1',   
creator                                             COMMENT '创建者', 
updator                                             COMMENT '更新者',
product_type2                                       COMMENT '投放类型（备用，包含app）',
app_type                                            COMMENT 'app类型',    
media_type_name                                     COMMENT '媒体类型名称',   
product_type_name                                   COMMENT '产品形态名称',   
city                                                COMMENT '城市名称',     
create_time                                         COMMENT '创建时间',     
agent                                               COMMENT '代理或者负责人'
)
COMMENT 'dsp账户维表'
AS SELECT
id, 
media_type, 
account,  
param_id, 
params,
status,   
creator, 
updator,
product_type2,
app_type,       
media_type_name,        
product_type_name,      
city,   
create_time,    
agent
FROM julive_dim.dim_dsp_account
;


DROP VIEW IF EXISTS tmp_bi.deal_dsp_account_history;
CREATE VIEW IF NOT EXISTS tmp_bi.deal_dsp_account_history (
id                                                  COMMENT '账户ID', 
media_type                                          COMMENT '媒体类型', 
account                                             COMMENT '账户',  
param_id                                            COMMENT '参数表id', 
params                                              COMMENT '请求api时需要传递的参数',
status                                              COMMENT '状态（1',   
creator                                             COMMENT '创建者', 
updator                                             COMMENT '更新者',
product_type2                                       COMMENT '投放类型（备用，包含app）',
app_type                                            COMMENT 'app类型',    
media_type_name                                     COMMENT '媒体类型名称',   
product_type_name                                   COMMENT '产品形态名称',   
city                                                COMMENT '城市名称',     
create_time                                         COMMENT '创建时间',     
agent                                               COMMENT '代理或者负责人',
p_date                                              COMMENT '快照时间',
pdate                                               COMMENT '分区时间'
)
COMMENT 'dsp账户维表历史快照表'
AS SELECT
*
FROM julive_dim.dim_dsp_account_history
;