CREATE OR REPLACE MACRO StockFunctionInfo(trade_day:=3) as TABLE 
SELECT *, percent_rank() OVER (
ORDER BY "涨停") AS "跌停分位"
FROM(SELECT "股票代码", count_star() FILTER (
WHERE("涨跌幅" >= 10)) AS "涨停"
FROM quant_data.stock.stock_data_daily
WHERE("日期" = ANY(SELECT DISTINCT "日期"
FROM quant_data.stock.stock_data_daily
ORDER BY "日期" DESC
LIMIT trade_day))
GROUP BY 1) AS t1 QUALIFY (percent_rank() OVER (
ORDER BY "涨停") > 0.5)
ORDER BY 2 DESC;

CREATE OR REPLACE MACRO StockHistRankQueryMinute15(start_date:='2024-01-01') as TABLE
WITH recent_stock_data_250d AS (SELECT *
FROM quant_data.stock.stock_data_15min AS t1
WHERE("日期" >=start_date::timestamp without time zone )
)
SELECT *,((SELECT count_star()
FROM recent_stock_data_250d
WHERE(("股票代码" = t1."股票代码")
AND ("日期" <= t1."日期")
AND ("收盘" <= t1."收盘"))) / count("股票名称") OVER (PARTITION BY "股票代码"
ORDER BY "日期")) AS historical_close_rank, row_number() OVER (PARTITION BY "股票代码"
ORDER BY "日期" DESC) AS stock_drn, row_number() OVER (PARTITION BY "股票代码"
ORDER BY "日期") AS stock_arn
FROM recent_stock_data_250d AS t1
ORDER BY "股票代码", "日期";


CREATE OR REPLACE MACRO StockHistRankQueryDaily(start_date:='2024-01-01') as TABLE
WITH recent_stock_data_250d AS (SELECT *
FROM quant_data.stock.stock_data_daily AS t1
WHERE("日期" >=start_date::timestamp without time zone ))
SELECT *,((SELECT count_star()
FROM recent_stock_data_250d
WHERE(("股票代码" = t1."股票代码")
AND ("日期" <= t1."日期")
AND ("收盘" <= t1."收盘"))) / count("股票名称") OVER (PARTITION BY "股票代码"
ORDER BY "日期")) AS historical_close_rank, row_number() OVER (PARTITION BY "股票代码"
ORDER BY "日期" DESC) AS stock_drn, row_number() OVER (PARTITION BY "股票代码"
ORDER BY "日期") AS stock_arn
FROM recent_stock_data_250d AS t1
ORDER BY "股票代码", "日期";


CREATE OR REPLACE MACRO stockMA(sub_day:=3) as TABLE
WITH "at" AS (SELECT *, kurtosis("收盘") OVER (PARTITION BY "股票代码"
ORDER BY "日期" RANGE BETWEEN to_days(CAST(trunc(CAST((sub_day - 1) AS DOUBLE)) AS INTEGER)) PRECEDING AND CURRENT ROW) AS "峰度", skewness("收盘") OVER (PARTITION BY "股票代码"
ORDER BY "日期" RANGE BETWEEN to_days(CAST(trunc(CAST((sub_day - 1) AS DOUBLE)) AS INTEGER)) PRECEDING AND CURRENT ROW) AS "偏度", avg("收盘") OVER (PARTITION BY "股票代码"
ORDER BY "日期" RANGE BETWEEN to_days(CAST(trunc(CAST((sub_day - 1) AS DOUBLE)) AS INTEGER)) PRECEDING AND CURRENT ROW) AS "均线", stddev_samp("收盘") OVER (PARTITION BY "股票代码"
ORDER BY "日期" RANGE BETWEEN to_days(CAST(trunc(CAST((sub_day - 1) AS DOUBLE)) AS INTEGER)) PRECEDING AND CURRENT ROW) AS "波动", row_number() OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) AS arn, row_number() OVER (PARTITION BY "股票代码"
ORDER BY "日期" DESC) AS drn
FROM quant_data.stock.stock_data_daily)
SELECT *,((SELECT count_star()
FROM "at"
WHERE(("股票代码" = t1."股票代码")
AND ("日期" <= t1."日期")
AND ("波动" <= t1."波动"))) / count_star() OVER (PARTITION BY "股票代码"
ORDER BY "日期")) AS "波动历史分位"
FROM "at" AS t1
ORDER BY "股票代码", "日期" DESC;


CREATE OR REPLACE MACRO StockLimitUp(trade_day:=3) as TABLE
SELECT *, percent_rank() OVER (
ORDER BY "涨停") AS "跌停分位"
FROM(SELECT "股票代码", count_star() FILTER (
WHERE("涨跌幅" >= 10)) AS "涨停"
FROM quant_data.stock.stock_data_daily
WHERE("日期" = ANY(SELECT DISTINCT "日期"
FROM quant_data.stock.stock_data_daily
ORDER BY "日期" DESC
LIMIT trade_day))
GROUP BY 1) AS t1
where "涨停">0
QUALIFY (percent_rank() OVER (
ORDER BY "涨停") > 0.5)
ORDER BY 2 DESC;


CREATE OR REPLACE MACRO StockMoveDaily(nrow:=3) as TABLE
SELECT *,((sum("成交额") OVER (PARTITION BY "股票代码"
ORDER BY "日期" ROWS BETWEEN nrow PRECEDING AND CURRENT ROW) / sum("成交量") OVER (PARTITION BY "股票代码"
ORDER BY "日期" ROWS BETWEEN nrow PRECEDING AND CURRENT ROW)) / 100) AS _Movedaily,(("成交额" / "成交量") / 100) AS _average
FROM quant_data.stock.stock_data_daily
ORDER BY "日期";

CREATE OR REPLACE MACRO StockTurnoverData(nrow := 120) as table
SELECT
股票名称,
股票代码,
日期,
换手率,
收盘,
row_number() OVER (PARTITION BY "股票代码"
ORDER BY "日期" DESC) AS stock_drn,
row_number() OVER (PARTITION BY "股票代码"
ORDER BY "日期") AS stock_arn,
avg(换手率) over f AS Avg_Turnover_Rate,
stddev_pop(换手率) over f AS Turnover_StdDev
FROM quant_data.stock.stock_data_daily
WINDOW
f AS (PARTITION BY "股票代码"
ORDER BY "日期"  RANGE BETWEEN to_days(CAST(trunc(CAST((nrow - 1) AS DOUBLE)) AS INTEGER)) PRECEDING AND CURRENT ROW);


CREATE OR REPLACE MACRO Calculate_Risk(nrow:=15) as TABLE
SELECT *,
stddev_pop(收盘)OVER (PARTITION BY "股票代码"
ORDER BY "日期" RANGE BETWEEN  to_days(CAST(trunc(CAST((nrow - 1) AS DOUBLE)) AS INTEGER))
PRECEDING AND CURRENT ROW) AS 风险度
FROM quant_data.stock.stock_data_daily;

create or replace macro backtest_data(start_date:='2024-11-01',end_date:='2024-11-20') as TABLE
select
股票名称, 股票代码,
日期,
收盘,
round(-1+columns(*exclude(股票名称, 股票代码,
日期,
收盘))/收盘,2)
from (
SELECT 股票名称, 股票代码,
日期,
收盘,
lead(收盘,1) over(partition by 股票代码 order by 日期) as one_daily,
lead(收盘,2) over(partition by 股票代码 order by 日期) as two_daily,
lead(收盘,3) over(partition by 股票代码 order by 日期) as three_daily,
lead(收盘,4) over(partition by 股票代码 order by 日期) as four_daily,
lead(收盘,5) over(partition by 股票代码 order by 日期) as five_daily,
lead(收盘,6) over(partition by 股票代码 order by 日期) as six_daily,
lead(收盘,7) over(partition by 股票代码 order by 日期) as seven_daily,
lead(收盘,8) over(partition by 股票代码 order by 日期) as eight_daily,
lead(收盘,9) over(partition by 股票代码 order by 日期) as nine_daily,
lead(收盘,10) over(partition by 股票代码 order by 日期) as ten_daily
FROM quant_data.stock.stock_data_daily
where 日期 between start_date::timestamp without time zone  and end_date::timestamp without time zone
);