CREATE VIEW stock.StockFunctionInfo AS SELECT oid, proname, pronamespace, prosqlbody
FROM pg_proc
WHERE(pronamespace = ANY(SELECT oid
FROM pg_namespace
WHERE(nspname = 'stock')));


-- stock.ViewSQL source

CREATE VIEW stock.ViewSQL AS SELECT *
FROM pg_views
WHERE(schemaname = 'stock');


-- stock.均线05_20_60 source

CREATE VIEW stock."均线05_20_60" AS SELECT *, floor((sum("换手率") OVER (PARTITION BY "股票代码" ORDER BY "日期" DESC) / 200)) AS "周转",(("成交额" / "成交量") / 100) AS _average,((sum("成交额") OVER (PARTITION BY "股票代码"
ORDER BY "日期" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) / sum("成交量") OVER (PARTITION BY "股票代码"
ORDER BY "日期" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) / 100) AS _5Movedaily,((sum("成交额") OVER (PARTITION BY "股票代码"
ORDER BY "日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) / sum("成交量") OVER (PARTITION BY "股票代码"
ORDER BY "日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)) / 100) AS _20Movedaily,((sum("成交额") OVER (PARTITION BY "股票代码"
ORDER BY "日期" ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) / sum("成交量") OVER (PARTITION BY "股票代码"
ORDER BY "日期" ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)) / 100) AS _60Movedaily
FROM stock.stock_data_daily
ORDER BY "股票代码", "日期";



-- stock.均线05_20_60_trend source

CREATE VIEW stock."均线05_20_60_trend" AS SELECT *,(percent_rank() OVER (PARTITION BY "股票代码", "周转"
ORDER BY "收盘" ASC) * 100) AS "历史分位", CASE WHEN (((lag(_average, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < _average)
AND (lag(_average, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag(_average, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC))
AND (lag(_average, 3, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag(_average, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (2)
WHEN (((lag(_average, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < _average)
AND (lag(_average, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag(_average, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (1)
WHEN (((lag(_average, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > _average)
AND (lag(_average, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag(_average, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC))
AND (lag(_average, 3, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag(_average, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (-2)
WHEN (((lag(_average, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > _average)
AND (lag(_average, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag(_average, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (-1)
ELSE 0
END AS _average_trend, CASE WHEN (((lag(_5Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < _5Movedaily)
AND (lag(_5Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag(_5Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC))
AND (lag(_5Movedaily, 3, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag(_5Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (2)
WHEN (((lag(_5Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < _5Movedaily)
AND (lag(_5Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag(_5Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (1)
WHEN (((lag(_5Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > _5Movedaily)
AND (lag(_5Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag(_5Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC))
AND (lag(_5Movedaily, 3, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag(_5Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (-2)
WHEN (((lag(_5Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > _5Movedaily)
AND (lag(_5Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag(_5Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (-1)
ELSE 0
END AS _5Movedaily_trend, CASE WHEN (((lag(_20Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < _20Movedaily)
AND (lag(_20Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag(_20Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC))
AND (lag(_20Movedaily, 3, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag(_20Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (2)
WHEN (((lag(_20Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < _20Movedaily)
AND (lag(_20Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag(_20Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (1)
WHEN (((lag(_20Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > _20Movedaily)
AND (lag(_20Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag(_20Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC))
AND (lag(_20Movedaily, 3, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag(_20Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (-2)
WHEN (((lag(_20Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > _20Movedaily)
AND (lag(_20Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag(_20Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (-1)
ELSE 0
END AS _20Movedaily_trend, CASE WHEN (((lag(_60Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < _60Movedaily)
AND (lag(_60Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag(_60Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC))
AND (lag(_60Movedaily, 3, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag(_60Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (2)
WHEN (((lag(_60Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < _60Movedaily)
AND (lag(_60Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag(_60Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (1)
WHEN (((lag(_60Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > _60Movedaily)
AND (lag(_60Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag(_60Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC))
AND (lag(_60Movedaily, 3, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag(_60Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (-2)
WHEN (((lag(_60Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > _60Movedaily)
AND (lag(_60Movedaily, 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag(_60Movedaily, 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (-1)
ELSE 0
END AS _60Movedaily_trend, CASE WHEN (((lag("成交量", 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < "成交量")
AND (lag("成交量", 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag("成交量", 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC))
AND (lag("成交量", 3, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag("成交量", 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (2)
WHEN (((lag("成交量", 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < "成交量")
AND (lag("成交量", 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) < lag("成交量", 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (1)
WHEN (((lag("成交量", 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > "成交量")
AND (lag("成交量", 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag("成交量", 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC))
AND (lag("成交量", 3, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag("成交量", 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (-2)
WHEN (((lag("成交量", 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > "成交量")
AND (lag("成交量", 2, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC) > lag("成交量", 1, NULL) OVER (PARTITION BY "股票代码"
ORDER BY "日期" ASC)))) THEN (-1)
ELSE 0
END AS "成交量_trend"
FROM stock."均线05_20_60";



-- stock.涨停次数分位120日 source

CREATE VIEW stock."涨停次数分位120日" AS SELECT *, percent_rank() OVER (
ORDER BY "涨停") AS "跌停分位"
FROM(SELECT "股票代码", count_star() FILTER (
WHERE("涨跌幅" >= 10)) AS "涨停"
FROM stock.stock_data_daily
WHERE("日期" BETWEEN (current_date() - 120) AND (current_date() + CAST('23:59:59.999' AS INTERVAL)))
GROUP BY 1) AS t1 QUALIFY (percent_rank() OVER (
ORDER BY "涨停") > 0.5)
ORDER BY 2 DESC;


-- stock.涨停演绎 source

CREATE VIEW stock."涨停演绎" AS SELECT *
FROM(SELECT "股票名称", "股票代码", "日期", "开盘", "收盘", "涨跌幅", CASE WHEN (("涨跌幅" >= 10)) THEN (lag("收盘", 1 IGNORE NULLS) OVER (PARTITION BY "股票代码"
ORDER BY "日期"))
ELSE NULL
END AS "前收盘"
FROM stock.stock_data_daily)
WHERE("前收盘" IS NOT NULL);