"""
AKShare MCP Server
基于 AKShare 的 Model Context Protocol 服务器，为 AI 助手提供中国金融市场数据访问能力
"""

import json
from typing import Any
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
import pandas as pd
import akshare as ak

# 创建 MCP 服务器实例
server = Server("akshare-mcp")

# ============================================
# 股票数据工具
# ============================================

@server.list_tools()
async def list_tools() -> list[Tool]:
    """列出所有可用的工具"""
    return [
        # 股票实时行情与搜索
        Tool(
            name="get_stock_zh_a_spot",
            description="获取所有 A 股实时行情数据，包括价格、涨跌幅、成交量等",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "股票代码，如 '600519' 或 '000001'，为空则返回所有股票"
                    }
                }
            }
        ),
        Tool(
            name="get_stock_zh_a_hist",
            description="获取 A 股历史 K 线数据，支持日线、周线、月线",
            inputSchema={
                "type": "object",
                "required": ["symbol", "period", "start_date", "end_date"],
                "properties": {
                    "symbol": {"type": "string", "description": "股票代码，如 '600519'"},
                    "period": {"type": "string", "description": "周期：daily/weekly/monthly", "enum": ["daily", "weekly", "monthly"]},
                    "start_date": {"type": "string", "description": "开始日期，格式：YYYYMMDD，如 '20230101'"},
                    "end_date": {"type": "string", "description": "结束日期，格式：YYYYMMDD，如 '20231231'"},
                    "adjust": {"type": "string", "description": "复权类型：qfq/none/hfq", "enum": ["qfq", "none", "hfq"]}
                }
            }
        ),
        Tool(
            name="search_stock_code",
            description="通过公司名称或股票代码搜索股票",
            inputSchema={
                "type": "object",
                "required": ["keyword"],
                "properties": {
                    "keyword": {"type": "string", "description": "搜索关键词，公司名称或股票代码"}
                }
            }
        ),

        # 股票财务数据
        Tool(
            name="get_stock_financial_indicator",
            description="获取股票财务指标，如 PE、PB、ROE、毛利率等",
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {"type": "string", "description": "股票代码，如 '600519'"},
                    "start_date": {"type": "string", "description": "开始日期，格式：YYYYMMDD"},
                    "end_date": {"type": "string", "description": "结束日期，格式：YYYYMMDD"}
                }
            }
        ),
        Tool(
            name="get_stock_balance_sheet",
            description="获取资产负债表数据",
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {"type": "string", "description": "股票代码，如 '600519'"}
                }
            }
        ),
        Tool(
            name="get_stock_income_statement",
            description="获取利润表数据",
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {"type": "string", "description": "股票代码，如 '600519'"}
                }
            }
        ),
        Tool(
            name="get_stock_cash_flow",
            description="获取现金流量表数据",
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {"type": "string", "description": "股票代码，如 '600519'"}
                }
            }
        ),
        Tool(
            name="get_stock_main_business",
            description="获取主营业务构成分析",
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {"type": "string", "description": "股票代码，如 '600519'"}
                }
            }
        ),
        Tool(
            name="get_stock_holders",
            description="获取前十大股东信息",
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {"type": "string", "description": "股票代码，如 '600519'"}
                }
            }
        ),

        # 股票新闻与板块
        Tool(
            name="get_stock_news",
            description="获取股票相关新闻",
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {"type": "string", "description": "股票代码，如 '600519'"},
                    "date": {"type": "string", "description": "日期，格式：YYYYMMDD，默认为最近日期"}
                }
            }
        ),
        Tool(
            name="get_stock_sector_industry",
            description="获取股票所属行业板块信息",
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {"type": "string", "description": "股票代码，如 '600519'"}
                }
            }
        ),

        # 指数数据
        Tool(
            name="get_stock_zh_index_spot",
            description="获取沪深主要指数实时行情，如上证指数、深证成指、创业板指等",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "指数代码，如 '000001'（上证指数）、'399001'（深证成指）"}
                }
            }
        ),
        Tool(
            name="get_index_hist",
            description="获取指数历史 K 线数据",
            inputSchema={
                "type": "object",
                "required": ["symbol", "period", "start_date", "end_date"],
                "properties": {
                    "symbol": {"type": "string", "description": "指数代码，如 '000001'"},
                    "period": {"type": "string", "description": "周期：daily/weekly/monthly", "enum": ["daily", "weekly", "monthly"]},
                    "start_date": {"type": "string", "description": "开始日期，格式：YYYYMMDD"},
                    "end_date": {"type": "string", "description": "结束日期，格式：YYYYMMDD"}
                }
            }
        ),
        Tool(
            name="get_index_components",
            description="获取指数成分股列表",
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {"type": "string", "description": "指数代码，如 '000300'（沪深300）"}
                }
            }
        ),

        # 基金数据
        Tool(
            name="get_fund_open_fund_daily",
            description="获取开放式基金每日净值数据",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "基金代码，如 '000001'"}
                }
            }
        ),
        Tool(
            name="get_fund_etf_spot",
            description="获取 ETF 实时行情数据",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_fund_etf_hist",
            description="获取 ETF 历史 K 线数据",
            inputSchema={
                "type": "object",
                "required": ["symbol", "period", "start_date", "end_date"],
                "properties": {
                    "symbol": {"type": "string", "description": "ETF 代码，如 '510300'"},
                    "period": {"type": "string", "description": "周期：daily/weekly/monthly", "enum": ["daily", "weekly", "monthly"]},
                    "start_date": {"type": "string", "description": "开始日期，格式：YYYYMMDD"},
                    "end_date": {"type": "string", "description": "结束日期，格式：YYYYMMDD"}
                }
            }
        ),
        Tool(
            name="search_fund_code",
            description="通过基金名称搜索基金代码",
            inputSchema={
                "type": "object",
                "required": ["keyword"],
                "properties": {
                    "keyword": {"type": "string", "description": "搜索关键词，如 '沪深300'、'白酒'"}
                }
            }
        ),

        # 资金流向
        Tool(
            name="get_stock_sector_fund_flow",
            description="获取行业板块资金流向排名",
            inputSchema={
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "日期，格式：YYYYMMDD"}
                }
            }
        ),
        Tool(
            name="get_stock_concept_fund_flow",
            description="获取概念板块资金流向排名",
            inputSchema={
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "日期，格式：YYYYMMDD"}
                }
            }
        ),
        Tool(
            name="get_stock_hsgt_hist",
            description="获取北向资金（沪深港通）历史流向数据",
            inputSchema={
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "开始日期，格式：YYYYMMDD"},
                    "end_date": {"type": "string", "description": "结束日期，格式：YYYYMMDD"}
                }
            }
        ),
        Tool(
            name="get_stock_hsgt_hold_stock",
            description="获取北向资金持仓排名",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "类型：north 或 south"}
                }
            }
        ),

        # 宏观经济
        Tool(
            name="get_macro_china_cpi",
            description="获取中国 CPI（消费者价格指数）数据",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_macro_china_ppi",
            description="获取中国 PPI（生产者价格指数）数据",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_macro_china_gdp",
            description="获取中国 GDP（国内生产总值）数据",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_macro_china_money_supply",
            description="获取中国货币供应量 M0、M1、M2 数据",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),

        # IPO 与分红
        Tool(
            name="get_stock_new_ipo",
            description="获取新股发行信息",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_stock_dividend",
            description="获取股票分红送转信息",
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {"type": "string", "description": "股票代码"}
                }
            }
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Any) -> TextContent:
    """执行工具调用"""
    try:
        if name == "get_stock_zh_a_spot":
            symbol = arguments.get("symbol")
            if symbol:
                df = ak.stock_zh_a_spot_em()
                df = df[df['代码'] == symbol] if symbol else df
            else:
                df = ak.stock_zh_a_spot_em()

        elif name == "get_stock_zh_a_hist":
            df = ak.stock_zh_a_hist(
                symbol=arguments["symbol"],
                period=arguments.get("period", "daily"),
                start_date=arguments["start_date"],
                end_date=arguments["end_date"],
                adjust=arguments.get("adjust", "qfq")
            )

        elif name == "search_stock_code":
            keyword = arguments["keyword"]
            df = ak.stock_info_a_code_name()
            df = df[df['name'].str.contains(keyword, na=False) | df['code'].str.contains(keyword, na=False)]

        elif name == "get_stock_financial_indicator":
            df = ak.stock_financial_analysis_indicator(
                symbol=arguments["symbol"],
                start_date=arguments.get("start_date", "20200101"),
                end_date=arguments.get("end_date", "20231231")
            )

        elif name == "get_stock_balance_sheet":
            df = ak.stock_balance_sheet_by_report_em(symbol=arguments["symbol"])

        elif name == "get_stock_income_statement":
            df = ak.stock_profit_sheet_by_report_em(symbol=arguments["symbol"])

        elif name == "get_stock_cash_flow":
            df = ak.stock_cash_flow_sheet_by_report_em(symbol=arguments["symbol"])

        elif name == "get_stock_main_business":
            df = ak.stock_profile(stock=arguments["symbol"])

        elif name == "get_stock_holders":
            df = ak.stock_top_10_holders(symbol=arguments["symbol"])

        elif name == "get_stock_news":
            symbol = arguments.get("symbol", "")
            date = arguments.get("date", "")
            if date:
                df = ak.stock_news_em(symbol=symbol, date=date)
            else:
                df = ak.stock_news_em(symbol=symbol)

        elif name == "get_stock_sector_industry":
            df = ak.stock_board_industry_name_em()
            stock_info = ak.stock_info_a_code_name()
            stock_info = stock_info[stock_info['code'] == arguments["symbol"]]
            if not stock_info.empty:
                # 查找该股票属于哪个板块
                industry_list = df['板块名称'].tolist()
                result = {"股票代码": arguments["symbol"], "可能板块": industry_list[:10]}
                return TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))
            return TextContent(type="text", text="未找到该股票的行业信息")

        elif name == "get_stock_zh_index_spot":
            symbol = arguments.get("symbol", "")
            if symbol:
                df = ak.stock_zh_index_spot_em(symbol=symbol)
            else:
                df = ak.stock_zh_index_spot_em()

        elif name == "get_index_hist":
            df = ak.stock_zh_index_daily(symbol=arguments["symbol"])

        elif name == "get_index_components":
            df = ak.index_stock_cons(symbol=arguments["symbol"])

        elif name == "get_fund_open_fund_daily":
            symbol = arguments.get("symbol", "")
            if symbol:
                df = ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")
            else:
                df = ak.fund_open_fund_daily_em()

        elif name == "get_fund_etf_spot":
            df = ak.fund_etf_spot_em()

        elif name == "get_fund_etf_hist":
            df = ak.fund_etf_hist_em(
                symbol=arguments["symbol"],
                period=arguments.get("period", "daily"),
                start_date=arguments["start_date"],
                end_date=arguments["end_date"]
            )

        elif name == "search_fund_code":
            keyword = arguments["keyword"]
            df = ak.fund_fund_info_ths()
            df = df[df['基金简称'].str.contains(keyword, na=False)]

        elif name == "get_stock_sector_fund_flow":
            date = arguments.get("date", "")
            if date:
                df = ak.stock_sector_fund_flow_rank(indicator=date)
            else:
                df = ak.stock_sector_fund_flow_rank(indicator="今日")

        elif name == "get_stock_concept_fund_flow":
            date = arguments.get("date", "")
            if date:
                df = ak.stock_concept_fund_flow_rank(indicator=date)
            else:
                df = ak.stock_concept_fund_flow_rank(indicator="今日")

        elif name == "get_stock_hsgt_hist":
            df = ak.stock_hsgt_north_net_flow_in(shell="北向", indicator="北向资金")
            if arguments.get("start_date"):
                df = df[df['日期'] >= arguments["start_date"]]
            if arguments.get("end_date"):
                df = df[df['日期'] <= arguments["end_date"]]

        elif name == "get_stock_hsgt_hold_stock":
            symbol = arguments.get("symbol", "北向")
            if symbol == "north" or symbol == "北向":
                df = ak.stock_hsgt_hold_stock_cninfo(symbol="北向", indicator="北向持股")

        elif name == "get_macro_china_cpi":
            df = ak.macro_china_cpi()

        elif name == "get_macro_china_ppi":
            df = ak.macro_china_ppi()

        elif name == "get_macro_china_gdp":
            df = ak.macro_china_gdp()

        elif name == "get_macro_china_money_supply":
            df = ak.macro_china_money_supply()

        elif name == "get_stock_new_ipo":
            df = ak.stock_ipo_summary_cninfo()

        elif name == "get_stock_dividend":
            df = ak.stock_dividend_details(symbol=arguments["symbol"])

        else:
            return TextContent(type="text", text=f"未知的工具: {name}")

        # 转换为易读的格式
        if isinstance(df, pd.DataFrame):
            if df.empty:
                return TextContent(type="text", text="数据为空")
            # 限制返回行数
            if len(df) > 100:
                df = df.head(100)
                warning = f"（仅显示前100行，共{len(df)}行）\n\n"
            else:
                warning = ""
            return TextContent(type="text", text=warning + df.to_string(index=False))
        else:
            return TextContent(type="text", text=str(df))

    except Exception as e:
        return TextContent(type="text", text=f"错误: {str(e)}\n\n可能的原因：\n1. 网络连接问题\n2. 数据接口暂时不可用\n3. 参数错误\n4. 股票代码不存在")


async def main():
    """主函数：启动 MCP 服务器"""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
