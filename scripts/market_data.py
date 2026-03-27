#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch latest market snapshot data before NotebookLM analysis.
"""

import datetime as dt
import json
import os
import subprocess
import time
from zoneinfo import ZoneInfo
from typing import Any

import httpx


EASTMONEY_FIELDS = [
    "f43",   # latest price
    "f44",   # high
    "f45",   # low
    "f46",   # open
    "f47",   # volume
    "f48",   # turnover
    "f57",   # code
    "f58",   # name
    "f84",   # total shares
    "f85",   # float shares
    "f116",  # market cap
    "f117",  # float market cap
    "f162",  # PE
    "f167",  # PB
    "f168",  # turnover rate
    "f169",  # price change
    "f170",  # price change percent
    "f171",  # amplitude percent
]


class MarketDataFetcher:
    """Fetch quote and key stats from Eastmoney quote endpoints."""

    def __init__(self):
        self.skill_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.workspace_root = os.path.dirname(os.path.dirname(os.path.dirname(self.skill_root)))
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://quote.eastmoney.com/",
            "Connection": "close",
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0, follow_redirects=True)

    def normalized_market(self, market: str) -> str:
        """Normalize internal market variants to the public market label."""
        if (market or "").upper().startswith("CN"):
            return "CN"
        return (market or "").upper()

    def eastmoney_secid(self, market: str, stock_input: str, stock_code: str | None = None) -> str:
        """Map skill inputs to Eastmoney secid values."""
        normalized_market = self.normalized_market(market)
        code = (stock_code or stock_input or "").strip().upper()

        if normalized_market == "US":
            return f"105.{code}"

        if normalized_market == "HK":
            return f"116.{code.zfill(5)}"

        exchange = "1" if code.startswith(("6", "9")) or code.startswith("688") else "0"
        return f"{exchange}.{code}"

    def quote_page_url(self, market: str, stock_input: str, stock_code: str | None = None) -> str:
        """Return the human-readable Eastmoney quote page."""
        normalized_market = self.normalized_market(market)
        code = (stock_code or stock_input or "").strip().upper()

        if normalized_market == "US":
            return f"https://quote.eastmoney.com/us/{code.lower()}.html"
        if normalized_market == "HK":
            return f"https://quote.eastmoney.com/hk/{code.zfill(5)}.html"

        prefix = "sh" if code.startswith(("6", "9")) or code.startswith("688") else "sz"
        return f"https://quote.eastmoney.com/{prefix}{code}.html"

    def currency_for_market(self, market: str) -> str:
        """Map market to trading currency label."""
        normalized_market = self.normalized_market(market)
        return {
            "CN": "CNY",
            "HK": "HKD",
            "US": "USD",
        }.get(normalized_market, "")

    def market_timezone(self, market: str) -> str:
        """Map market to its primary exchange timezone."""
        normalized_market = self.normalized_market(market)
        return {
            "CN": "Asia/Shanghai",
            "HK": "Asia/Hong_Kong",
            "US": "America/New_York",
        }.get(normalized_market, "UTC")

    def market_session_label(self, market: str, generated_at: dt.datetime | None = None) -> str:
        """Provide a simple hint about whether the fetch happened during the trading session."""
        normalized_market = self.normalized_market(market)
        now_utc = generated_at or dt.datetime.now(dt.timezone.utc)
        local_now = now_utc.astimezone(ZoneInfo(self.market_timezone(normalized_market)))
        weekday = local_now.weekday()
        hhmm = local_now.hour * 100 + local_now.minute
        if weekday >= 5:
            return "weekend_or_holiday"
        if normalized_market == "CN":
            if 930 <= hhmm <= 1130 or 1300 <= hhmm <= 1500:
                return "intraday"
            return "off_session"
        if normalized_market == "HK":
            if 930 <= hhmm <= 1200 or 1300 <= hhmm <= 1600:
                return "intraday"
            return "off_session"
        if normalized_market == "US":
            if 930 <= hhmm <= 1600:
                return "intraday"
            return "off_session"
        return "unknown"

    def price_scale(self, market: str) -> int:
        """Eastmoney uses different integer scales by market."""
        normalized_market = self.normalized_market(market)
        return 100 if normalized_market == "CN" else 1000

    def ratio_scale(self) -> int:
        """Ratios like PE/PB/turnover are scaled by 100."""
        return 100

    def _pause_before_retry(self, attempt: int):
        """Add a small backoff between retries for flaky upstream responses."""
        time.sleep(0.4 * attempt)

    def fetch_quote(self, secid: str) -> dict[str, Any]:
        """Fetch structured quote data from Eastmoney push2 API."""
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "secid": secid,
            "fields": ",".join(EASTMONEY_FIELDS),
        }
        last_error = None

        for attempt in range(1, 4):
            try:
                response = self.client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
                data = payload.get("data") or {}
                if data:
                    return data
                last_error = ValueError(f"Eastmoney returned empty data for {secid}")
            except Exception as exc:
                last_error = exc
                self._pause_before_retry(attempt)

        curl_url = f"{url}?secid={secid}&fields={','.join(EASTMONEY_FIELDS)}"
        for attempt in range(1, 4):
            try:
                result = subprocess.run(
                    [
                        "curl",
                        "-L",
                        "--max-time",
                        "20",
                        "--retry",
                        "2",
                        "--retry-all-errors",
                        "--http1.1",
                        curl_url,
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                payload = json.loads(result.stdout)
                data = payload.get("data") or {}
                if data:
                    return data
                last_error = ValueError(f"Eastmoney curl fallback returned empty data for {secid}")
            except Exception as exc:
                last_error = exc
                self._pause_before_retry(attempt)

        raise RuntimeError(
            "Failed to fetch Eastmoney quote "
            f"(secid={secid}, source=Eastmoney push2, last_error={last_error})"
        )

    def xueqiu_symbol(self, market: str, stock_input: str, stock_code: str | None = None) -> str:
        """Map skill inputs to the symbol format expected by Xueqiu/OpenCLI."""
        normalized_market = self.normalized_market(market)
        code = (stock_code or stock_input or "").strip().upper()
        if normalized_market == "US":
            return code
        if normalized_market == "HK":
            return code.zfill(5)
        prefix = "SH" if code.startswith(("6", "9")) or code.startswith("688") else "SZ"
        return f"{prefix}{code}"

    def fetch_xueqiu_quote(self, symbol: str) -> dict[str, Any]:
        """Fetch fallback quote data through the local OpenCLI Xueqiu integration."""
        command = (
            "source ~/.zshrc >/dev/null 2>&1; "
            f"{os.path.join(self.workspace_root, '.gemini/skills/opencli-skill/scripts/run-opencli.sh')} "
            f"xueqiu stock --symbol {symbol} -f json"
        )
        last_error = None

        for attempt in range(1, 3):
            try:
                result = subprocess.run(
                    ["/bin/zsh", "-lc", command],
                    capture_output=True,
                    text=True,
                    check=True,
                    cwd=self.workspace_root,
                )
                data = json.loads(result.stdout)
                if isinstance(data, list) and data:
                    item = data[0]
                    if item:
                        return item
                last_error = ValueError(f"Xueqiu returned empty data for {symbol}")
            except Exception as exc:
                last_error = exc
                self._pause_before_retry(attempt)

        raise RuntimeError(
            "Failed to fetch Xueqiu quote "
            f"(symbol={symbol}, source=OpenCLI xueqiu stock, last_error={last_error})"
        )

    def _scaled(self, value: Any, scale: int) -> float | None:
        """Convert Eastmoney integer-scaled fields to decimal values."""
        if value in (None, "", "-"):
            return None
        try:
            return float(value) / scale
        except (TypeError, ValueError):
            return None

    def _raw_number(self, value: Any) -> float | None:
        """Normalize plain numeric values without extra scaling."""
        if value in (None, "", "-"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _percent_to_number(self, value: Any) -> float | None:
        """Convert percent strings like '-1.01%' to float values."""
        if value in (None, "", "-"):
            return None
        try:
            return float(str(value).replace("%", "").strip())
        except ValueError:
            return None

    def _chinese_amount_to_number(self, value: Any) -> float | None:
        """Convert strings like '2090.02亿' or '3.66万亿' to raw numeric amounts."""
        if value in (None, "", "-"):
            return None
        text = str(value).strip()
        multipliers = {
            "万亿": 1_000_000_000_000,
            "亿": 100_000_000,
            "万": 10_000,
        }
        for unit, multiplier in multipliers.items():
            if text.endswith(unit):
                try:
                    return float(text[: -len(unit)]) * multiplier
                except ValueError:
                    return None
        return self._raw_number(text)

    def build_snapshot(self, market: str, stock_input: str, stock_name: str = None, stock_code: str = None) -> dict[str, Any]:
        """Fetch and normalize a market snapshot."""
        normalized_market = self.normalized_market(market)
        secid = self.eastmoney_secid(normalized_market, stock_input, stock_code=stock_code)
        generated_at = dt.datetime.now(dt.timezone.utc)
        try:
            quote = self.fetch_quote(secid)
            price_scale = self.price_scale(normalized_market)
            ratio_scale = self.ratio_scale()

            current_price = self._scaled(quote.get("f43"), price_scale)
            price_change = self._scaled(quote.get("f169"), price_scale)
            previous_close = current_price - price_change if current_price is not None and price_change is not None else None

            snapshot = {
                "generated_at": generated_at.isoformat(),
                "generated_at_local": generated_at.astimezone(
                    ZoneInfo(self.market_timezone(normalized_market))
                ).isoformat(),
                "market": normalized_market,
                "input": stock_input,
                "stock_code": stock_code or quote.get("f57") or stock_input,
                "stock_name": stock_name or quote.get("f58") or stock_input,
                "exchange": secid.split(".", 1)[0],
                "currency": self.currency_for_market(normalized_market),
                "current_price": current_price,
                "price_change": price_change,
                "price_change_percent": self._scaled(quote.get("f170"), ratio_scale),
                "previous_close": previous_close,
                "open": self._scaled(quote.get("f46"), price_scale),
                "day_high": self._scaled(quote.get("f44"), price_scale),
                "day_low": self._scaled(quote.get("f45"), price_scale),
                "amplitude_percent": self._scaled(quote.get("f171"), ratio_scale),
                "market_cap": self._raw_number(quote.get("f116")),
                "float_market_cap": self._raw_number(quote.get("f117")),
                "shares_outstanding": self._raw_number(quote.get("f84")),
                "float_shares": self._raw_number(quote.get("f85")),
                "volume": self._raw_number(quote.get("f47")),
                "turnover": self._raw_number(quote.get("f48")),
                "turnover_rate": self._scaled(quote.get("f168"), ratio_scale),
                "trailing_pe": self._scaled(quote.get("f162"), ratio_scale),
                "price_to_book": self._scaled(quote.get("f167"), ratio_scale),
                "eastmoney_secid": secid,
                "source_name": "东方财富 push2 行情接口",
                "source_url": self.quote_page_url(normalized_market, stock_input, stock_code=stock_code),
                "market_timezone": self.market_timezone(normalized_market),
                "market_session_hint": self.market_session_label(normalized_market, generated_at),
                "freshness_note": "运行时实时抓取；若抓取发生在非交易时段，则更接近最近收盘/最近成交口径。",
                "validation_provider": None,
                "validation_notes": [],
            }
            try:
                xueqiu_symbol = self.xueqiu_symbol(normalized_market, stock_input, stock_code=stock_code)
                validation_quote = self.fetch_xueqiu_quote(xueqiu_symbol)
                validation_price = self._raw_number(validation_quote.get("price"))
                snapshot["validation_provider"] = "雪球 OpenCLI 行情兜底"
                if validation_price is not None and snapshot["current_price"] is not None:
                    diff = abs(snapshot["current_price"] - validation_price)
                    pct = diff / snapshot["current_price"] if snapshot["current_price"] else 0
                    snapshot["validation_notes"].append(
                        f"东方财富价格={snapshot['current_price']}, 雪球价格={validation_price}, 差异={diff:.4f} ({pct:.2%})"
                    )
                    snapshot["validation_status"] = "cross_checked_ok" if pct <= 0.03 else "cross_checked_warning"
                else:
                    snapshot["validation_status"] = "cross_check_unavailable"
                    snapshot["validation_notes"].append("无法从雪球获取可比价格，未完成双源价格校验。")
            except Exception as validation_error:
                snapshot["validation_status"] = "cross_check_failed"
                snapshot["validation_notes"].append(f"双源校验失败: {validation_error}")
            return snapshot
        except Exception as eastmoney_error:
            xueqiu_symbol = self.xueqiu_symbol(normalized_market, stock_input, stock_code=stock_code)
            try:
                quote = self.fetch_xueqiu_quote(xueqiu_symbol)
                current_price = self._raw_number(quote.get("price"))
                market_cap = self._chinese_amount_to_number(quote.get("marketCap"))
                shares_outstanding = None
                if current_price and market_cap:
                    shares_outstanding = market_cap / current_price

                return {
                    "generated_at": generated_at.isoformat(),
                    "generated_at_local": generated_at.astimezone(
                        ZoneInfo(self.market_timezone(normalized_market))
                    ).isoformat(),
                    "market": normalized_market,
                    "input": stock_input,
                    "stock_code": stock_code or stock_input,
                    "stock_name": stock_name or quote.get("name") or stock_input,
                    "exchange": quote.get("symbol"),
                    "currency": self.currency_for_market(normalized_market),
                    "current_price": current_price,
                    "price_change": None,
                    "price_change_percent": self._percent_to_number(quote.get("changePercent")),
                    "previous_close": None,
                    "open": None,
                    "day_high": None,
                    "day_low": None,
                    "amplitude_percent": None,
                    "market_cap": market_cap,
                    "float_market_cap": None,
                    "shares_outstanding": shares_outstanding,
                    "float_shares": None,
                    "volume": None,
                    "turnover": None,
                    "turnover_rate": None,
                    "trailing_pe": None,
                    "price_to_book": None,
                    "eastmoney_secid": secid,
                    "source_name": "雪球 OpenCLI 行情兜底",
                    "source_url": quote.get("url") or f"https://xueqiu.com/S/{xueqiu_symbol}",
                    "market_timezone": self.market_timezone(normalized_market),
                    "market_session_hint": self.market_session_label(normalized_market, generated_at),
                    "freshness_note": "运行时抓取；若处于非交易时段，则通常更接近最近收盘/最近成交口径。",
                    "validation_provider": None,
                    "validation_status": "single_source_fallback",
                    "validation_notes": [f"东方财富抓取失败，已切换雪球兜底: {eastmoney_error}"],
                }
            except Exception as xueqiu_error:
                raise RuntimeError(
                    "Failed to fetch market snapshot after trying both providers "
                    f"(eastmoney_error={eastmoney_error}; xueqiu_error={xueqiu_error})"
                ) from xueqiu_error

    def close(self):
        self.client.close()


def format_number(value: Any) -> str:
    """Human friendly number formatting."""
    if value is None or value == "":
        return "N/A"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if abs(value) >= 1000:
            return f"{value:,.2f}"
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value)


def snapshot_to_markdown(snapshot: dict[str, Any]) -> str:
    """Create a NotebookLM-friendly markdown snapshot."""
    lines = [
        "# 最新市场数据快照",
        "",
        "以下数据用于在财报分析前补充最新市场上下文，帮助判断估值、击球区和总股本相关指标。",
        "",
        f"- 生成时间: {snapshot.get('generated_at', 'N/A')}",
        f"- 市场本地时间: {snapshot.get('generated_at_local', 'N/A')}",
        f"- 市场: {snapshot.get('market', 'N/A')}",
        f"- 公司: {snapshot.get('stock_name', 'N/A')}",
        f"- 股票代码: {snapshot.get('stock_code', 'N/A')}",
        f"- Eastmoney SecID: {snapshot.get('eastmoney_secid', 'N/A')}",
        f"- 数据来源: {snapshot.get('source_name', 'N/A')}",
        f"- 行情页面: {snapshot.get('source_url', 'N/A')}",
        f"- 交易时段提示: {snapshot.get('market_session_hint', 'N/A')}",
        "",
        "## 关键市场数据",
        "",
        f"- 最新股价: {format_number(snapshot.get('current_price'))} {snapshot.get('currency', '')}".strip(),
        f"- 涨跌额: {format_number(snapshot.get('price_change'))}",
        f"- 涨跌幅: {format_number(snapshot.get('price_change_percent'))}%",
        f"- 昨收: {format_number(snapshot.get('previous_close'))}",
        f"- 开盘价: {format_number(snapshot.get('open'))}",
        f"- 日内区间: {format_number(snapshot.get('day_low'))} - {format_number(snapshot.get('day_high'))}",
        f"- 振幅: {format_number(snapshot.get('amplitude_percent'))}%",
        f"- 总市值: {format_number(snapshot.get('market_cap'))}",
        f"- 流通市值: {format_number(snapshot.get('float_market_cap'))}",
        f"- 总股本: {format_number(snapshot.get('shares_outstanding'))}",
        f"- 流通股本: {format_number(snapshot.get('float_shares'))}",
        f"- 成交量: {format_number(snapshot.get('volume'))}",
        f"- 成交额: {format_number(snapshot.get('turnover'))}",
        f"- 换手率: {format_number(snapshot.get('turnover_rate'))}%",
        "",
        "## 估值辅助指标",
        "",
        f"- PE(TTM): {format_number(snapshot.get('trailing_pe'))}",
        f"- PB(MRQ/最新): {format_number(snapshot.get('price_to_book'))}",
        "",
        "## 数据新鲜度与校验",
        "",
        f"- 新鲜度说明: {snapshot.get('freshness_note', 'N/A')}",
        f"- 双源校验状态: {snapshot.get('validation_status', 'N/A')}",
        f"- 校验来源: {snapshot.get('validation_provider', 'N/A')}",
    ]
    for note in snapshot.get("validation_notes") or []:
        lines.append(f"- 校验备注: {note}")
    lines.extend([
        "",
        "## 使用要求",
        "",
        "- 进行财报分析时，必须结合这份市场快照与财报原文共同判断。",
        "- 如果市场快照与财报披露期存在时间差，需要明确说明这是最新市场数据而非财报期末口径。",
        "- 在估值、击球区和每股指标判断时，优先引用这份快照中的股价、市值和总股本信息。",
        "- 若双源校验出现明显差异，必须在结论中提示行情数据可能存在时点差或口径差。",
        "",
        "## 原始 JSON",
        "",
        "```json",
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        "```",
    ])
    return "\n".join(lines) + "\n"
