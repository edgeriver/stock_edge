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

CREATE OR REPLACE MACRO StockHistRankQueryMinute15(sub_day:=3) as TABLE
WITH recent_stock_data_250d AS (SELECT *
FROM quant_data.stock.stock_data_15min AS t1
WHERE("日期" BETWEEN ((current_date() - sub_day) + 1) AND (current_date() + CAST('23:59:59.99999' AS INTERVAL))))SELECT *,((SELECT count_star()
FROM recent_stock_data_250d
WHERE(("股票代码" = t1."股票代码")
AND ("日期" <= t1."日期")
AND ("收盘" <= t1."收盘"))) / count("股票名称") OVER (PARTITION BY "股票代码"
ORDER BY "日期")) AS historical_close_rank, row_number() OVER (PARTITION BY "股票代码"
ORDER BY "日期" DESC) AS stock_drn, row_number() OVER (PARTITION BY "股票代码"
ORDER BY "日期") AS stock_arn
FROM recent_stock_data_250d AS t1
ORDER BY "股票代码", "日期";


CREATE OR REPLACE MACRO StockHistRankQueryDaily(sub_day:=3) as TABLE
WITH recent_stock_data_250d AS (SELECT *
FROM quant_data.stock.stock_data_daily AS t1
WHERE("日期" BETWEEN ((current_date() - sub_day) + 1) AND (current_date() + CAST('23:59:59.99999' AS INTERVAL))))SELECT *,((SELECT count_star()
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
GROUP BY 1) AS t1 QUALIFY (percent_rank() OVER (
ORDER BY "涨停") > 0.5)
ORDER BY 2 DESC;


CREATE OR REPLACE MACRO StockMoveDaily(nrow:=3) as TABLE
SELECT *,((sum("成交额") OVER (PARTITION BY "股票代码"
ORDER BY "日期" ROWS BETWEEN nrow PRECEDING AND CURRENT ROW) / sum("成交量") OVER (PARTITION BY "股票代码"
ORDER BY "日期" ROWS BETWEEN nrow PRECEDING AND CURRENT ROW)) / 100) AS _Movedaily,(("成交额" / "成交量") / 100) AS _average
FROM quant_data.stock.stock_data_daily
ORDER BY "日期"