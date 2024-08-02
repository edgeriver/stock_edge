init_indicator='''
DROP SCHEMA IF EXISTS indicators CASCADE;
CREATE SCHEMA IF NOT EXISTS indicators;
DROP SCHEMA IF EXISTS myschema CASCADE;
CREATE SCHEMA IF NOT EXISTS myschema;
use  myschema;
CREATE table  myschema.trade_trend as(
select * 
from (
select *  ,
percent_rank()over(partition by 股票代码,日期::date order by 收盘  asc) as 日内百分比排名,

1-1.0*(SELECT COUNT(*)  
     FROM  read_parquet('./src/stock_data_15min.parquet') s2 
     WHERE s2.股票代码 = t1.股票代码 AND s2.日期 <= t1.日期 AND s2.收盘 >t1.收盘)/COUNT(股票名称)OVER(PARTITION BY 股票代码 ORDER BY 日期 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  ) as history_per,

ROW_NUMBER() over(partition by 股票代码 order by 日期 desc) as date_rn,
ROW_NUMBER() over(partition by 股票代码 order by 日期 asc) as date_asc_rn,
case when 收盘- lag(收盘,1,NULL)over(f ) >0 then 1 
when 收盘- lag(收盘,1,NULL)over(f ) <0  then -1
else 0 end
as diff_Change,
case when 
lag(收盘,1,NULL)over(f )<收盘
and lag(收盘,2,NULL)over(f )<lag(收盘,1,NULL)over(f )
and lag(收盘,3,NULL)over(f )<lag(收盘,2,NULL)over(f )
THEN 2
when 
lag(收盘,1,NULL)over(f )<收盘
and lag(收盘,2,NULL)over(f )<lag(收盘,1,NULL)over(f )
THEN 1
 when 
lag(收盘,1,NULL)over(f )>收盘
and lag(收盘,2,NULL)over(f )>lag(收盘,1,NULL)over(f )
and lag(收盘,3,NULL)over(f )>lag(收盘,2,NULL)over(f )
THEN -2
when 
lag(收盘,1,NULL)over(f )>收盘
and lag(收盘,2,NULL)over(f )>lag(收盘,1,NULL)over(f )
THEN -1
else 0 end as growth_trend,
FROM  read_parquet('./src/stock_data_15min.parquet') t1
-- where 日期 between '2023-11-01' and '2024-01-08 14:30:00'
window  f as (partition by 股票代码 order by 日期 asc)
) t1);

create table industry_trend as 
select 行业板块,count(股票代码) as stock_amount FROM read_parquet('./src/all_realtime.parquet')
where 涨跌幅>0 and 行业板块 is not null
group by 行业板块
order by 2 desc;

create table stock_inno as 
select *  FROM read_parquet('./src/inno.parquet');

create table stock_daily as 
select *  FROM read_parquet('./src/all_realtime.parquet');

'''
init_indicator2='''
CREATE table  myschema.trade_trend_daily as(
select * 
from (
select *  ,
percent_rank()over(partition by 股票代码,date_trunc('month',日期) order by 收盘  asc) as 月内百分比排名,

1-1.0*(SELECT COUNT(*) 
     FROM  read_parquet('./src/stock_data_daily.parquet') s2 
     WHERE s2.股票代码 = t1.股票代码 AND s2.日期 <= t1.日期 AND s2.收盘 >t1.收盘)/COUNT(股票名称)OVER(PARTITION BY 股票代码 ORDER BY 日期 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  ) as history_per,

ROW_NUMBER() over(partition by 股票代码 order by 日期 desc) as date_rn,
ROW_NUMBER() over(partition by 股票代码 order by 日期 asc) as date_asc_rn,
case when 收盘- lag(收盘,1,NULL)over(f ) >0 then 1 
when 收盘- lag(收盘,1,NULL)over(f ) <0  then -1
else 0 end
as diff_Change,
case when 
lag(收盘,1,NULL)over(f )<收盘
and lag(收盘,2,NULL)over(f )<lag(收盘,1,NULL)over(f )
and lag(收盘,3,NULL)over(f )<lag(收盘,2,NULL)over(f )
THEN 2
when 
lag(收盘,1,NULL)over(f )<收盘
and lag(收盘,2,NULL)over(f )<lag(收盘,1,NULL)over(f )
THEN 1
 when 
lag(收盘,1,NULL)over(f )>收盘
and lag(收盘,2,NULL)over(f )>lag(收盘,1,NULL)over(f )
and lag(收盘,3,NULL)over(f )>lag(收盘,2,NULL)over(f )
THEN -2
when 
lag(收盘,1,NULL)over(f )>收盘
and lag(收盘,2,NULL)over(f )>lag(收盘,1,NULL)over(f )
THEN -1
else 0 end as growth_trend,
FROM  read_parquet('./src/stock_data_daily.parquet') t1
-- where 日期 between '2023-11-01' and '2024-01-08 14:30:00'
window  f as (partition by 股票代码 order by 日期 asc)
) t1);
'''


obv_indicator = '''drop table if exists indicators.obv;
create table indicators.obv as
(select * from 
(SELECT *
,case when 
lag(OBV,1,NULL)over(f )<OBV
and lag(OBV,2,NULL)over(f )<lag(OBV,1,NULL)over(f )
and lag(OBV,3,NULL)over(f )<lag(OBV,2,NULL)over(f )
THEN 2
when 
lag(OBV,1,NULL)over(f )<OBV
and lag(OBV,2,NULL)over(f )<lag(OBV,1,NULL)over(f )
THEN 1
 when 
lag(OBV,1,NULL)over(f )>OBV
and lag(OBV,2,NULL)over(f )>lag(OBV,1,NULL)over(f )
and lag(OBV,3,NULL)over(f )>lag(OBV,2,NULL)over(f )
THEN -2
when 
lag(OBV,1,NULL)over(f )>OBV
and lag(OBV,2,NULL)over(f )>lag(OBV,1,NULL)over(f )
THEN -1
else 0 end as OBV_trend
FROM (

select 股票名称,股票代码,日期,
成交量,
diff_Change,
sum(成交量*diff_Change)over(partition by 股票代码 order by 日期 ) OBV
,date_rn 
,date_asc_rn 
FROM myschema.trade_trend)
window  f as (partition by 股票代码 order by 日期 asc))
where date_rn=1 and OBV_trend>0)'''
rsi_calculation = '''drop table if exists indicators.rsi;

create table  indicators.rsi as(

with at as(

	SELECT 
股票代码,
日期,
	收盘,
涨跌额,
case when 涨跌额>0 then abs(涨跌额) else 0 end as gain,
case when 涨跌额<0 then abs(涨跌额) else 0 end as loss,
	date_asc_rn,
	date_rn
FROM myschema.trade_trend)
, bt as(

WITH RECURSIVE rsi_recursive AS (

	SELECT 
    股票代码,
    日期,
       		收盘,
       		涨跌额,
        first(gain)over(partition by 股票代码 order by 日期 asc) AS stock_gain, -- 初始值为第一个价格
        first(loss)over(partition by 股票代码 order by 日期 asc) AS stock_loss, -- 初始值为第一个价格
        	date_asc_rn,
        	date_rn
    FROM at
     WHERE date_asc_rn = 1
   
    
    UNION ALL
    
    SELECT 
    p.股票代码,
        p.日期,
        p.收盘,
        p.涨跌额,
        (p.gain * (2.0 / (6 + 1))) + (r.stock_gain * (1 - (2.0 / (6 + 1)))) AS stock_gain,
   				 							(p.loss * (2.0 / (6 + 1))) + (r.stock_loss * (1 - (2.0 / (6 + 1)))) AS stock_loss,
        p.date_asc_rn,
        p.date_rn
    FROM at p
    JOIN rsi_recursive r ON p.date_asc_rn = r.date_asc_rn+1 and p.股票代码=r.股票代码

)
SELECT 股票代码,日期, 收盘,涨跌额,stock_gain,stock_loss,stock_gain*100.0/(stock_gain+stock_loss)as rsi,date_asc_rn,date_rn
FROM rsi_recursive
order by 股票代码,日期
)
select * from bt)'''
bollinger_indicator = '''drop table if exists indicators.boll;
create table indicators.boll
as 

(select *　from(
select *,
close_avg+2*close_stddev as boll_up,
close_avg-2*close_stddev as boll_dn,
收盘-close_avg+2*close_stddev  as 支持线,
case when 
lag(close_avg,1,NULL)over(f )<close_avg
and lag(close_avg,2,NULL)over(f )<lag(close_avg,1,NULL)over(f )
and lag(close_avg,3,NULL)over(f )<lag(close_avg,2,NULL)over(f )
THEN 2
when 
lag(close_avg,1,NULL)over(f )<close_avg
and lag(close_avg,2,NULL)over(f )<lag(close_avg,1,NULL)over(f )
THEN 1
 when 
lag(close_avg,1,NULL)over(f )>close_avg
and lag(close_avg,2,NULL)over(f )>lag(close_avg,1,NULL)over(f )
and lag(close_avg,3,NULL)over(f )>lag(close_avg,2,NULL)over(f )
THEN -2
when 
lag(close_avg,1,NULL)over(f )>close_avg
and lag(close_avg,2,NULL)over(f )>lag(close_avg,1,NULL)over(f )
THEN -1
else 0 end as close_avg_trend,
case when 
lag(close_stddev,1,NULL)over(f )<close_stddev
and lag(close_stddev,2,NULL)over(f )<lag(close_stddev,1,NULL)over(f )
and lag(close_stddev,3,NULL)over(f )<lag(close_stddev,2,NULL)over(f )
THEN 2
when 
lag(close_stddev,1,NULL)over(f )<close_stddev
and lag(close_stddev,2,NULL)over(f )<lag(close_stddev,1,NULL)over(f )
THEN 1
 when 
lag(close_stddev,1,NULL)over(f )>close_stddev
and lag(close_stddev,2,NULL)over(f )>lag(close_stddev,1,NULL)over(f )
and lag(close_stddev,3,NULL)over(f )>lag(close_stddev,2,NULL)over(f )
THEN -2
when 
lag(close_stddev,1,NULL)over(f )>close_stddev
and lag(close_stddev,2,NULL)over(f )>lag(close_stddev,1,NULL)over(f )
THEN -1
else 0 end as close_stddev_trend
from
(
select 股票名称,股票代码,日期,开盘,收盘,最高,最低,
date_rn,
date_asc_rn,
avg(收盘)over f as close_avg,
stddev_pop(收盘)over f close_stddev
FROM myschema.trade_trend
window f as (partition by 股票代码 order by 日期 rows between 19 preceding and current row ))
window  f as (partition by 股票代码 order by 日期 asc))
where date_rn=1 and 支持线>0)'''
macd_model_calculation ='''DROP TABLE IF EXISTS  indicators.ema;
DROP TABLE IF EXISTS  indicators.stock_macd_12_26_10;
create table indicators.ema as(
WITH RECURSIVE ema_recursive AS (

	SELECT 
    股票代码,
    日期,
       		收盘,
       		最低,
        first(最低)over(partition by 股票代码 order by 日期 asc) AS ema12, -- 初始值为第一个价格
         first(最低)over(partition by 股票代码 order by 日期 asc) AS ema26, -- 初始值为第一个价格
        	date_asc_rn,
        	date_rn
    FROM myschema.trade_trend
     WHERE date_asc_rn = 1
   
    
    UNION ALL
    
    SELECT 
    p.股票代码,
        p.日期,
        p.收盘,
        p.最低,
        (p.最低 * (2.0 / (12 + 1))) + (r.ema12 * (1 - (2.0 / (12 + 1)))) AS ema12,
        (p.最低 * (2.0 / (26 + 1))) + (r.ema26 * (1 - (2.0 / (26 + 1)))) AS ema26,
        p.date_asc_rn,
        p.date_rn
    FROM myschema.trade_trend p
    JOIN ema_recursive r ON p.date_asc_rn = r.date_asc_rn+1 and p.股票代码=r.股票代码

)
SELECT 股票代码,日期, 收盘,最低,ema12,ema26,date_asc_rn,date_rn, ema12-ema26 as diff
FROM ema_recursive
);
create table indicators.stock_macd_12_26_10 as(
WITH RECURSIVE diff_recursive AS (
select 股票代码,日期,收盘,最低,ema12,ema26,date_asc_rn,date_rn,diff,diff as dea
from indicators.ema where date_asc_rn =1
union ALL 
select p.股票代码,p.日期,p.收盘,p.最低,p.ema12,p.ema26,p.date_asc_rn,p.date_rn,p.diff,
(p.diff * (2.0 / (9 + 1))) + (r.dea * (1 - (2.0 / (9 + 1)))) AS dea
from indicators.ema p join diff_recursive r ON p.date_asc_rn = r.date_asc_rn+1 and p.股票代码=r.股票代码
)
 SELECT 股票代码, 日期, 收盘, 最低,ema12, ema26,diff, dea,2*(diff-dea) as bar,date_asc_rn, date_rn from diff_recursive
order by 股票代码,日期
)'''

stock_data_update_task='''
insert or replace into quant_data.stock.stock_data_daily
SELECT 股票名称, 股票代码, 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
from read_parquet('./src/stock_data_daily.parquet');

insert or replace into quant_data.stock.stock_data_15min 
SELECT 股票名称, 股票代码, 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
from read_parquet('./src/stock_data_15min.parquet');

insert or replace into quant_data.stock.fund_flow_history
select * from read_parquet('./src/fund_flow_history.parquet');
 
insert or replace into stock.fund_flow_minute
SELECT 股票名称,股票代码,
strptime(时间, ['%Y-%m-%d %H:%M'])::TIMESTAMP AS 时间,
主力净流入,小单净流入,中单净流入,大单净流入,超大单净流入
FROM read_parquet('./src/fund_flow_minute.parquet');


INSERT or replace into stock.stock_chip_plate
select * from 
read_parquet('./src/stock_chip_plate.parquet');
'''


stocks_analysis='''
use  myschema;
select * from myschema.stock_daily t
where 股票代码 in(
SELECT 股票代码 from  quant_data.stock.StockHistRankQueryDaily(sub_day:=90)
where historical_close_rank<0.2 and 涨跌幅<0 and stock_drn=1 and 日期=current_date()
)
and 股票代码 in(select 股票代码 from read_parquet('./src/realtime.parquet'))
and 股票代码=any(
SELECT 股票代码 from quant_data.stock.StockLimitUp(trade_day:=120)
)
and (股票代码 like '60%' OR 股票代码 like '00%')

and 最新价<any(SELECT 前收盘
FROM quant_data.stock.涨停演绎 where t.股票代码 =股票代码 )


'''