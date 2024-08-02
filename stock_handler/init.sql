-- quant_data.myschema.industry_trend definition
create schema if not exists mysqlschema;
create schema if not exists stock;
CREATE TABLE myschema.industry_trend("行业板块" VARCHAR, stock_amount BIGINT);

-- quant_data.myschema.stock_daily definition

CREATE TABLE myschema.stock_daily("股票代码" VARCHAR, "股票名称" VARCHAR, "涨跌幅" DOUBLE, "最新价" DOUBLE, "最高" DOUBLE, "最低" DOUBLE, "今开" DOUBLE, "涨跌额" DOUBLE, "换手率" DOUBLE, "量比" DOUBLE, "动态市盈率" DOUBLE, "成交量" DOUBLE, "成交额" DOUBLE, "昨日收盘" DOUBLE, "总市值" DOUBLE, "流通市值" DOUBLE, Roe DOUBLE, "负债率" DOUBLE, "滚动市盈率" DOUBLE, "行业板块" VARCHAR, "行情ID" VARCHAR, "市场类型" VARCHAR, "更新时间" VARCHAR, "最新交易日" VARCHAR);

-- quant_data.myschema.stock_inno definition

CREATE TABLE myschema.stock_inno("股票代码" VARCHAR, "股票名称" VARCHAR, "涨跌幅" DOUBLE, "最新价" DOUBLE, "最高" DOUBLE, "最低" DOUBLE, "今开" DOUBLE, "涨跌额" DOUBLE, "换手率" DOUBLE, "量比" DOUBLE, "动态市盈率" DOUBLE, "成交量" DOUBLE, "成交额" DOUBLE, "昨日收盘" DOUBLE, "总市值" DOUBLE, "流通市值" DOUBLE, "市场编号" BIGINT, "行情ID" VARCHAR, "市场类型" VARCHAR, "更新时间" VARCHAR, "最新交易日" VARCHAR);


-- quant_data.myschema.trade_trend definition

CREATE TABLE myschema.trade_trend("股票名称" VARCHAR, "股票代码" VARCHAR, "日期" TIMESTAMP, "开盘" DOUBLE, "收盘" DOUBLE, "最高" DOUBLE, "最低" DOUBLE, "成交量" BIGINT, "成交额" DOUBLE, "振幅" DOUBLE, "涨跌幅" DOUBLE, "涨跌额" DOUBLE, "换手率" DOUBLE, "日内百分比排名" DOUBLE, history_per DOUBLE, date_rn BIGINT, date_asc_rn BIGINT, diff_Change INTEGER, growth_trend INTEGER);

-- quant_data.myschema.trade_trend_daily definition

CREATE TABLE myschema.trade_trend_daily("股票名称" VARCHAR, "股票代码" VARCHAR, "日期" TIMESTAMP, "开盘" DOUBLE, "收盘" DOUBLE, "最高" DOUBLE, "最低" DOUBLE, "成交量" BIGINT, "成交额" DOUBLE, "振幅" DOUBLE, "涨跌幅" DOUBLE, "涨跌额" DOUBLE, "换手率" DOUBLE, "月内百分比排名" DOUBLE, history_per DOUBLE, date_rn BIGINT, date_asc_rn BIGINT, diff_Change INTEGER, growth_trend INTEGER);

-- quant_data.stock.fund_flow_history definition

CREATE TABLE stock.fund_flow_history("股票名称" VARCHAR, "股票代码" VARCHAR, "日期" TIMESTAMP, "主力净流入" DOUBLE, "小单净流入" DOUBLE, "中单净流入" DOUBLE, "大单净流入" DOUBLE, "超大单净流入" DOUBLE, "主力净流入占比" DOUBLE, "小单流入净占比" DOUBLE, "中单流入净占比" DOUBLE, "大单流入净占比" DOUBLE, "超大单流入净占比" DOUBLE, "收盘价" DOUBLE, "涨跌幅" DOUBLE);

-- quant_data.stock.fund_flow_minute definition

CREATE TABLE stock.fund_flow_minute("股票名称" VARCHAR, "股票代码" VARCHAR, "时间" TIMESTAMP, "主力净流入" DOUBLE, "小单净流入" DOUBLE, "中单净流入" DOUBLE, "大单净流入" DOUBLE, "超大单净流入" DOUBLE);

-- quant_data.stock.stock_chip_plate definition

CREATE TABLE stock.stock_chip_plate("代码" VARCHAR, "名称" VARCHAR, "时间" TIMESTAMP, "涨跌额" DOUBLE, "涨跌幅" DOUBLE, "最新价" DOUBLE, "昨收" DOUBLE, "今开" DOUBLE, "开盘" DOUBLE, "最高" DOUBLE, "最低" DOUBLE, "均价" DOUBLE, "涨停价" DOUBLE, "跌停价" DOUBLE, "换手率" DOUBLE, "成交量" DOUBLE, "成交额" DOUBLE, "卖1价" DOUBLE, "卖2价" DOUBLE, "卖3价" DOUBLE, "卖4价" DOUBLE, "卖5价" DOUBLE, "买1价" DOUBLE, "买2价" DOUBLE, "买3价" DOUBLE, "买4价" DOUBLE, "买5价" DOUBLE, "卖1数量" DOUBLE, "卖2数量" DOUBLE, "卖3数量" DOUBLE, "卖4数量" DOUBLE, "卖5数量" DOUBLE, "买1数量" DOUBLE, "买2数量" DOUBLE, "买3数量" DOUBLE, "买4数量" DOUBLE, "买5数量" DOUBLE);

-- quant_data.stock.stock_data_15min definition

CREATE TABLE stock.stock_data_15min("股票名称" VARCHAR, "股票代码" VARCHAR, "日期" TIMESTAMP, "开盘" DOUBLE, "收盘" DOUBLE, "最高" DOUBLE, "最低" DOUBLE, "成交量" BIGINT, "成交额" DOUBLE, "振幅" DOUBLE, "涨跌幅" DOUBLE, "涨跌额" DOUBLE, "换手率" DOUBLE);

-- quant_data.stock.stock_data_daily definition

CREATE TABLE stock.stock_data_daily("股票名称" VARCHAR, "股票代码" VARCHAR, "日期" TIMESTAMP, "开盘" DOUBLE, "收盘" DOUBLE, "最高" DOUBLE, "最低" DOUBLE, "成交量" BIGINT, "成交额" DOUBLE, "振幅" DOUBLE, "涨跌幅" DOUBLE, "涨跌额" DOUBLE, "换手率" DOUBLE);

-- quant_data.stock.stock_intraday_volume definition

CREATE TABLE stock.stock_intraday_volume("股票名称" VARCHAR, "股票代码" VARCHAR, "时间" TIMESTAMP, "昨收" DOUBLE, "成交价" DOUBLE, "成交量" HUGEINT, "单数" HUGEINT);