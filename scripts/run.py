#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FinancialReport2NotebookLLM - Multi-Market Orchestrator
Supports A-share, US, and HK markets with Markdown conversion.
"""

import sys
import os
import json
import tempfile
import shutil
import re
import datetime
import time
import subprocess

# --- VENV BOOTSTRAP ---
# 彻底解决路径问题：自动寻找并使用本地虚拟环境
script_dir = os.path.dirname(os.path.abspath(__file__))
skill_root = os.path.dirname(script_dir)
runtime_root = os.path.abspath(
    os.environ.get("FINANCIAL_REPORT_NOTEBOOKLM_RUNTIME_ROOT", os.getcwd())
)
venv_python = os.path.join(skill_root, ".venv", "bin", "python")

if os.path.exists(venv_python) and sys.executable != venv_python:
    # 如果检测到本地虚拟环境且当前未在使用，则自动重启脚本
    os.execl(venv_python, venv_python, *sys.argv)
# ----------------------

# Add scripts directory to path
sys.path.insert(0, script_dir)

BANK_NAME_HINTS = (
    "银行",
    "bank",
    "bancorp",
    "bancshares",
    "bankshares",
    "savings bank",
)

BANK_CORE_REPORT_MARKERS = (
    "不良贷款",
    "关注类贷款",
    "拨备覆盖率",
    "拨贷比",
    "核心一级资本充足率",
    "资本充足率",
    "风险加权资产",
    "净利息收入",
    "净息差",
    "已逾期未减值",
    "贷款损失准备",
    "allowance for credit losses",
    "allowance for loan losses",
    "non-performing loan",
    "nonperforming loan",
    "net interest income",
    "net interest margin",
    "common equity tier 1",
    "tier 1 capital",
    "risk-weighted assets",
    "provision for credit losses",
)

BANK_SUPPLEMENTAL_REPORT_MARKERS = (
    "吸收存款",
    "客户存款",
    "发放贷款和垫款",
    "客户贷款和垫款",
    "不良贷款",
    "存放中央银行款项",
    "同业及其他金融机构存放款项",
    "customer deposits",
    "loans and advances",
)


def looks_like_bank_name(value: str) -> bool:
    """Check whether a company name or ticker strongly hints at a bank."""
    text = (value or "").strip()
    if not text:
        return False
    lower = text.lower()
    if "银行" in text:
        return True
    return any(keyword in lower for keyword in BANK_NAME_HINTS)


def read_report_excerpt(file_path: str, max_chars: int = 50000) -> str:
    """Read a short excerpt from a text-like report file for profile detection."""
    lower_path = file_path.lower()
    if not lower_path.endswith((".md", ".txt", ".html")):
        return ""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read(max_chars)
    except Exception:
        return ""


def detect_bank_stock(stock_input: str, stock_name: str, files: list) -> tuple[bool, list]:
    """Detect whether the target company should use the bank-specific analysis profile."""
    reasons = []
    if looks_like_bank_name(stock_input) or looks_like_bank_name(stock_name):
        reasons.append("company_name")

    core_marker_hits = set()
    supplemental_marker_hits = set()
    for file_path in files[:5]:
        combined_text = os.path.basename(file_path) + "\n" + read_report_excerpt(file_path)
        combined_lower = combined_text.lower()
        for marker in BANK_CORE_REPORT_MARKERS:
            if marker.lower() in combined_lower:
                core_marker_hits.add(marker)
        for marker in BANK_SUPPLEMENTAL_REPORT_MARKERS:
            if marker.lower() in combined_lower:
                supplemental_marker_hits.add(marker)
        if len(core_marker_hits) >= 2 or (len(core_marker_hits) >= 1 and len(supplemental_marker_hits) >= 2):
            break

    if len(core_marker_hits) >= 2:
        reasons.append(f"core_markers={', '.join(sorted(core_marker_hits)[:5])}")
    elif len(core_marker_hits) >= 1 and len(supplemental_marker_hits) >= 2:
        reasons.append(
            "report_markers="
            + ", ".join(sorted((core_marker_hits | supplemental_marker_hits))[:5])
        )

    return bool(reasons), reasons


def get_analysis_assets(market: str, is_bank_stock_profile: bool) -> tuple[str, str]:
    """Choose the system prompt and analysis profile."""
    if is_bank_stock_profile:
        return "bank_financial_analyst_prompt.md", "bank"
    if market == "US":
        return "us_financial_analyst_prompt.md", "us"
    return "financial_analyst_prompt.md", "general"


def build_report_prompt(
    stock_name: str,
    is_bank_stock_profile: bool,
    include_recent_developments: bool = False,
) -> str:
    """Build the stock-specific report prompt and leave structure to the system prompt."""
    recent_context = "以及近期重大事件摘要" if include_recent_developments else ""
    if is_bank_stock_profile:
        if include_recent_developments:
            return f"请基于已上传的全部财报（重点结合近5年年报及最新定期报告）、最新市场数据快照{recent_context}，为{stock_name}生成一份全方位的银行股投资备忘录。"
        return f"请基于已上传的全部财报（重点结合近5年年报及最新定期报告）和最新市场数据快照，为{stock_name}生成一份全方位的银行股投资备忘录。"
    if include_recent_developments:
        return f"请基于已上传的全部财报（重点结合近5年年报及最新定期报告）、最新市场数据快照{recent_context}，为{stock_name}生成一份全方位的投资备忘录。"
    return f"请基于已上传的全部财报（重点结合近5年年报及最新定期报告）和最新市场数据快照，为{stock_name}生成一份全方位的投资备忘录。"


def detect_market(stock_input: str) -> str:
    """Detect market based on input string"""
    # Ticker (US): Letters, dots, and dashes (e.g., BRK.B, BF-B)
    if re.match(r"^[A-Za-z.-]+$", stock_input):
        return "US"
    # HK Code: 5 digits (can start with 0)
    if re.match(r"^\d{5}$", stock_input):
        return "HK"
    # A-share Code: 6 digits
    if re.match(r"^\d{6}$", stock_input):
        return "CN"
    # Default to CN name lookup
    return "CN_NAME"


def normalize_market_label(market: str) -> str:
    """Normalize internal market variants for user-facing labels."""
    if (market or "").upper().startswith("CN"):
        return "CN"
    return (market or "UNK").strip().upper()


def load_analysis_plan(plan_path: str) -> list:
    """Load post-upload analysis prompts from a JSON asset."""
    with open(plan_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("questions", [])


def ensure_output_dir(skill_root_path: str, market: str, stock_input: str) -> str:
    """Create a persistent directory for analysis outputs."""
    path = os.path.join(skill_root_path, "outputs", f"{market}_{stock_input}")
    os.makedirs(path, exist_ok=True)
    return path


def get_runtime_data_dir(market: str, stock_input: str) -> str:
    """Create a persistent runtime data directory outside the skill source tree."""
    path = os.path.join(runtime_root, "data", f"{market}_{stock_input}")
    os.makedirs(path, exist_ok=True)
    return path


def get_runtime_outputs_dir(market: str, stock_input: str) -> str:
    """Create a persistent runtime outputs directory outside the skill source tree."""
    path = os.path.join(runtime_root, "outputs", f"{market}_{stock_input}")
    os.makedirs(path, exist_ok=True)
    return path


def write_text(path: str, content: str):
    """Write text to disk using UTF-8."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def get_notebook_url(notebook_id: str) -> str:
    """Build the direct NotebookLM URL for one notebook."""
    return f"https://notebooklm.google.com/notebook/{notebook_id}"


def copy_text_to_clipboard(text: str) -> tuple[bool, str]:
    """Copy text to the macOS clipboard when available."""
    try:
        result = subprocess.run(
            ["pbcopy"],
            input=text,
            text=True,
            capture_output=True,
            check=False,
        )
    except Exception as exc:
        return False, str(exc)

    if result.returncode == 0:
        return True, ""
    return False, (result.stderr or result.stdout or f"pbcopy exited with {result.returncode}").strip()


def open_notebook_in_browser(notebook_id: str) -> tuple[bool, str]:
    """Open the target NotebookLM notebook in the default browser."""
    notebook_url = get_notebook_url(notebook_id)
    try:
        result = subprocess.run(
            ["open", notebook_url],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:
        return False, str(exc)

    if result.returncode == 0:
        return True, notebook_url
    error_text = (result.stderr or result.stdout or f"open exited with {result.returncode}").strip()
    return False, error_text


def prepare_manual_summary_fallback(
    analysis_dir: str,
    notebook_id: str,
    stock_name: str,
    summary_prompt: str,
    summary_output: str,
) -> str:
    """Persist manual fallback instructions when automated summary generation fails."""
    notebook_url = get_notebook_url(notebook_id)
    prompt_path = os.path.join(analysis_dir, "01_manual_summary_prompt.txt")
    instructions_path = os.path.join(analysis_dir, "01_manual_summary_fallback.md")

    write_text(prompt_path, summary_prompt + "\n")

    clipboard_ok, clipboard_error = copy_text_to_clipboard(summary_prompt)
    browser_ok, browser_result = open_notebook_in_browser(notebook_id)

    lines = [
        f"# {stock_name} 手动总结兜底",
        "",
        "NotebookLM 自动总结失败，已切换为手动兜底流程。",
        "",
        f"- Notebook ID: {notebook_id}",
        f"- Notebook 链接: {notebook_url}",
        f"- 提示词文件: {prompt_path}",
        f"- 已复制到剪贴板: {'是' if clipboard_ok else '否'}",
        f"- 已尝试打开浏览器: {'是' if browser_ok else '否'}",
    ]

    if not clipboard_ok and clipboard_error:
        lines.append(f"- 剪贴板错误: {clipboard_error}")
    if not browser_ok and browser_result:
        lines.append(f"- 浏览器打开错误: {browser_result}")

    lines.extend(
        [
            "",
            "## 操作步骤",
            "",
            "1. 打开上面的 NotebookLM 链接。",
            "2. 将已复制到剪贴板的提示词粘贴到输入框。",
            "3. 手动获取最终总结，并按需保存到输出目录。",
            "",
            "## 本次提示词",
            "",
            "```text",
            summary_prompt,
            "```",
            "",
            "## 自动调用失败信息",
            "",
            "```text",
            summary_output.strip() or "No error details returned.",
            "```",
            "",
        ]
    )
    write_text(instructions_path, "\n".join(lines))
    return instructions_path


def sync_download_failures_to_outputs(market: str, stock_input: str, failures: list):
    """Copy conversion/download error logs into the final outputs directory."""
    if not failures:
        return

    output_dir = get_runtime_outputs_dir(market, stock_input)
    summary_lines = ["# 下载与转换失败汇总", ""]

    for item in failures:
        title = item.get("title", "unknown")
        stage = item.get("stage", "unknown")
        error = item.get("error", "unknown error")
        path = item.get("path")
        summary_lines.append(f"- [{stage}] {title}: {error}")

        if path and os.path.exists(path) and path.endswith(".txt"):
            target_path = os.path.join(output_dir, os.path.basename(path))
            shutil.copyfile(path, target_path)

    write_text(
        os.path.join(output_dir, "00_download_failures.md"),
        "\n".join(summary_lines) + "\n",
    )


def log_stage_start(label: str) -> float:
    """Print a stage start marker and return the timer baseline."""
    started_at = time.time()
    timestamp = datetime.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n⏱️ [{timestamp}] START {label}")
    return started_at


def log_stage_end(label: str, started_at: float):
    """Print a stage end marker with elapsed seconds."""
    elapsed = time.time() - started_at
    timestamp = datetime.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    print(f"✅ [{timestamp}] END {label} ({elapsed:.2f}s)")


def resolve_existing_notebook(notebook_id: str, notebook_state: dict) -> tuple[bool, dict, dict]:
    """Retry notebook reuse checks before falling back to notebook creation."""
    from upload import get_existing_source_map, list_notebooks

    notebook_title_map = {}
    last_source_map = {}

    for attempt in range(1, 4):
        notebooks_ok, notebook_list = list_notebooks()
        if notebooks_ok:
            notebook_title_map = {
                item.get("id"): item.get("title", "")
                for item in notebook_list
                if item.get("id")
            }

        state_ok, existing_source_map = get_existing_source_map(notebook_id)
        if state_ok:
            return True, existing_source_map, notebook_title_map

        last_source_map = existing_source_map or {}
        print(
            f"⚠️ Notebook reuse check failed for {notebook_id} "
            f"(attempt {attempt}/3); retrying..."
        )
        if attempt < 3:
            time.sleep(2)

    return False, last_source_map, notebook_title_map


def format_notebook_title(market: str, stock_code: str, stock_name: str) -> str:
    """Build a stable notebook title for easy scanning and deduping."""
    safe_market = normalize_market_label(market)
    safe_code = (stock_code or "UNKNOWN").strip().upper()
    safe_name = " ".join((stock_name or safe_code).strip().split())
    return f"[{safe_market}] {safe_code} {safe_name} - 财报分析"


def title_needs_rename(current_title: str, target_title: str) -> bool:
    """Return whether an existing notebook title should be normalized."""
    current = (current_title or "").strip()
    target = (target_title or "").strip()
    return bool(current and target and current != target)


def get_notebook_state_path(output_dir: str) -> str:
    """Return the persistent notebook state path for one company."""
    company_key = os.path.basename(output_dir.rstrip(os.sep))
    return os.path.join(runtime_root, "data", company_key, "notebook_state.json")


def load_notebook_state(output_dir: str) -> dict:
    """Load notebook reuse state if it exists."""
    path = get_notebook_state_path(output_dir)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_notebook_state(output_dir: str, state: dict):
    """Persist notebook reuse state for future incremental runs."""
    state_path = get_notebook_state_path(output_dir)
    os.makedirs(os.path.dirname(state_path), exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def fetch_market_snapshot(market: str, stock_input: str, stock_name: str, stock_code: str, output_dir: str) -> tuple:
    """Fetch latest market snapshot and write a temporary markdown source."""
    from market_data import MarketDataFetcher, snapshot_to_markdown

    analysis_dir = get_runtime_outputs_dir(market, stock_input)
    analysis_snapshot_path = os.path.join(analysis_dir, "00_latest_market_snapshot.md")
    analysis_error_path = os.path.join(analysis_dir, "00_latest_market_snapshot_error.txt")
    fetcher = MarketDataFetcher()
    try:
        snapshot = fetcher.build_snapshot(
            market=market,
            stock_input=stock_input,
            stock_name=stock_name,
            stock_code=stock_code,
        )
    except Exception as e:
        fetcher.close()
        error_text = (
            "Latest market snapshot fetch failed.\n\n"
            f"market={market}\n"
            f"stock_input={stock_input}\n"
            f"stock_name={stock_name}\n"
            f"stock_code={stock_code}\n"
            f"error={e}\n"
        )
        write_text(analysis_error_path, error_text)
        if os.path.exists(analysis_snapshot_path):
            os.remove(analysis_snapshot_path)
        print(f"⚠️ Failed to fetch latest market snapshot: {e}")
        print(f"📝 Saved market snapshot error log: {analysis_error_path}")
        return None, None

    fetcher.close()
    snapshot_markdown = snapshot_to_markdown(snapshot)
    snapshot_path = os.path.join(output_dir, "00_latest_market_snapshot.md")
    write_text(snapshot_path, snapshot_markdown)
    write_text(analysis_snapshot_path, snapshot_markdown)
    if os.path.exists(analysis_error_path):
        os.remove(analysis_error_path)
    print(f"📈 Saved latest market snapshot: {snapshot_path}")
    print(f"📈 Saved latest market snapshot output: {analysis_snapshot_path}")
    return snapshot, snapshot_path


def summary_is_empty(summary_text: str) -> bool:
    """Detect placeholder or empty NotebookLM summaries."""
    normalized = (summary_text or "").strip()
    return not normalized or normalized.lower() == "no summary available"


def build_summary_fallback(stock_name: str) -> str:
    """Provide a clear fallback when NotebookLM summary is unavailable."""
    return (
        f"# {stock_name} NotebookLM 摘要暂不可用\n\n"
        "NotebookLM 本次没有返回可用 summary，因此这里不再写入空白占位内容。\n\n"
        "请直接查看后续问答文件（`02_*` 到 `07_*`）以及 `99_notebooklm_report.md`，"
        "其中已经包含本轮分析的主要结论。\n"
    )


def file_has_nonempty_content(path: str) -> bool:
    """Return whether a text artifact exists and contains non-whitespace content."""
    if not os.path.exists(path):
        return False
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return bool(f.read().strip())
    except Exception:
        return False


def analysis_outputs_need_refresh(output_dir: str) -> tuple[bool, list]:
    """Decide whether saved NotebookLM outputs are missing or unusable."""
    required_files = [
        "01_notebook_summary.md",
        "98_notebook_artifacts.json",
        "99_notebooklm_report.md",
    ]
    missing = [
        filename
        for filename in required_files
        if not file_has_nonempty_content(os.path.join(output_dir, filename))
    ]
    if missing:
        return True, missing

    summary_path = os.path.join(output_dir, "01_notebook_summary.md")
    try:
        with open(summary_path, "r", encoding="utf-8", errors="ignore") as f:
            summary_text = f.read()
    except Exception:
        return True, ["01_notebook_summary.md"]

    if summary_is_empty(summary_text):
        return True, ["01_notebook_summary.md"]

    fallback_markers = (
        "NotebookLM 本次没有返回可用 summary",
        "Summary failed:",
    )
    if any(marker in summary_text for marker in fallback_markers):
        return True, ["01_notebook_summary.md"]

    return False, []


def filter_cached_files(market: str, files: list) -> list:
    """Filter out stale cached files that are not real financial reports."""
    if market != "HK":
        return files

    from hk_downloader import HkexDownloader

    downloader = HkexDownloader()
    filtered = []
    for file_path in files:
        title = os.path.splitext(os.path.basename(file_path))[0]
        if downloader.is_financial_report_title(title):
            filtered.append(file_path)
        else:
            print(f"⚠️ Skipping non-financial cached file: {os.path.basename(file_path)}")
    return filtered


def hk_cache_needs_refresh(files: list) -> bool:
    """港股通常至少需要多期财报；单份缓存大多意味着上次运行中断。"""
    return len(files) < 3


def get_cn_report_markers(report_plan: dict) -> list:
    """Return expected A-share report markers for cache freshness checks."""
    markers = []

    for year in report_plan.get("annual_years", []):
        markers.append(
            {
                "label": f"{year} annual",
                "patterns": [f"{year}年年度报告", f"{year}年年报"],
            }
        )

    periodic_patterns = {
        "q1": [
            "{year}年第一季度报告",
            "{year}年一季度报告",
            "{year}年第一季度",
            "{year}年一季度",
        ],
        "semi": [
            "{year}年半年度报告",
            "{year}年中期报告",
            "{year}年半年度",
            "{year}年中期",
        ],
        "q3": [
            "{year}年第三季度报告",
            "{year}年三季度报告",
            "{year}年第三季度",
            "{year}年三季度",
        ],
    }

    for report_type, year in report_plan.get("periodic_targets", {}).items():
        if not year:
            continue
        markers.append(
            {
                "label": f"{year} {report_type}",
                "patterns": [pattern.format(year=year) for pattern in periodic_patterns[report_type]],
            }
        )

    return markers


def get_missing_cn_reports(files: list, report_plan: dict) -> list:
    """Check whether cached A-share files cover the latest expected report set."""
    basenames = [os.path.splitext(os.path.basename(file_path))[0] for file_path in files]
    missing = []

    for marker in get_cn_report_markers(report_plan):
        if not any(any(pattern in basename for pattern in marker["patterns"]) for basename in basenames):
            missing.append(marker["label"])

    return missing


def dedupe_file_paths(files: list[str]) -> list[str]:
    """Preserve order while removing duplicate absolute file paths."""
    deduped = []
    seen = set()
    for file_path in files:
        normalized = os.path.abspath(file_path)
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(file_path)
    return deduped


def write_summary_input_preview(
    market: str,
    stock_input: str,
    stock_name: str,
    stock_code: str,
    is_bank_stock_profile: bool,
    market_snapshot: dict | None,
    recent_developments: list[dict] | None,
    include_recent_developments: bool,
) -> str:
    """Save the exact summary inputs for review before asking NotebookLM to summarize."""
    analysis_dir = get_runtime_outputs_dir(market, stock_input)
    preview_path = os.path.join(analysis_dir, "00_summary_input_preview.md")

    lines = [
        "# 总结前确认输入",
        "",
        f"- 公司: {stock_name}",
        f"- 股票代码: {stock_code}",
        f"- 市场: {normalize_market_label(market)}",
        f"- 生成时间: {datetime.datetime.now().astimezone().isoformat()}",
        "",
        "## 市场快照",
        "",
    ]

    if market_snapshot:
        local_time = market_snapshot.get("generated_at_local") or market_snapshot.get("generated_at") or "N/A"
        lines.extend(
            [
                f"- 抓取时间: {local_time}",
                f"- 交易时段提示: {market_snapshot.get('market_session_hint', 'N/A')}",
                f"- 最新股价: {market_snapshot.get('current_price', 'N/A')} {market_snapshot.get('currency', '')}".rstrip(),
                f"- 昨收: {market_snapshot.get('previous_close', 'N/A')}",
                f"- 日内区间: {market_snapshot.get('day_low', 'N/A')} - {market_snapshot.get('day_high', 'N/A')}",
                f"- 总市值: {market_snapshot.get('market_cap', 'N/A')}",
                f"- 总股本: {market_snapshot.get('shares_outstanding', 'N/A')}",
                f"- PE(TTM): {market_snapshot.get('trailing_pe', 'N/A')}",
                f"- PB: {market_snapshot.get('price_to_book', 'N/A')}",
                f"- 双源校验状态: {market_snapshot.get('validation_status', 'N/A')}",
                f"- 校验备注: {market_snapshot.get('validation_notes', 'N/A')}",
            ]
        )
    else:
        lines.append("- 本次没有成功抓取市场快照。")

    lines.extend(["", "## 近期重大事件", ""])
    if include_recent_developments and recent_developments:
        lines.append("- 本次将纳入总结输入。")
        lines.append("")
        for idx, item in enumerate(recent_developments[:5], start=1):
            lines.append(f"{idx}. {item.get('date', 'N/A')} | {item.get('title', 'N/A')}")
    else:
        lines.append("- 本次默认忽略，不纳入总结输入。")
        if recent_developments:
            top_score = max(item.get("score", -999) for item in recent_developments)
            lines.append(f"- 原因: 最近事件最高信号分为 {top_score}，未达到纳入阈值。")
        else:
            lines.append("- 原因: 当前未抓取到结构化的近期重大事件。")

    lines.extend(
        [
            "",
            "## 本次实际总结提示词",
            "",
            "```text",
            build_directional_summary_prompt(
                stock_name,
                is_bank_stock_profile=is_bank_stock_profile,
                include_recent_developments=include_recent_developments,
            ),
            "```",
            "",
        ]
    )

    write_text(preview_path, "\n".join(lines))
    return preview_path


def _format_event_time(value) -> str:
    """Normalize various announcement timestamps into readable strings."""
    if value in (None, "", "-"):
        return "N/A"
    try:
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 10_000_000_000:
                ts /= 1000
            return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass
    return str(value)


def _event_signal_score(title: str, normalized_market: str) -> int:
    """Rough heuristic to keep higher-signal events near the top."""
    text = (title or "").strip()
    if not text:
        return -999

    strong_markers = (
        "利润分配",
        "分红",
        "回购",
        "增持",
        "减持",
        "配股",
        "定向增发",
        "再融资",
        "重大事项",
        "风险提示",
        "业绩预告",
        "业绩快报",
        "不良资产",
        "资本补充",
        "可转债",
        "处罚",
        "诉讼",
        "收购",
        "出售",
        "停牌",
        "复牌",
        "并购",
    )
    medium_markers = (
        "董事会决议",
        "股东会决议",
        "任职资格核准",
        "续聘",
        "聘任",
    )
    routine_markers = (
        "独立董事述职",
        "可持续发展报告",
        "社会责任报告",
        "内部控制评价报告",
        "内部控制审计报告",
        "英文",
        "摘要",
        "监事会工作报告",
    )

    score = 0
    for marker in strong_markers:
        if marker in text:
            score += 5
    for marker in medium_markers:
        if marker in text:
            score += 2
    for marker in routine_markers:
        if marker in text:
            score -= 4

    if normalized_market == "US":
        upper = text.upper()
        for marker in ("8-K", "10-Q", "10-K", "20-F", "6-K", "EARNINGS", "DIVIDEND", "BUYBACK"):
            if marker in upper:
                score += 4
        for marker in ("SC 13", "S-8", "424B", "PX14A6G", "DEFA14A"):
            if marker in upper:
                score -= 3

    return score


def has_material_recent_developments(items: list[dict] | None) -> bool:
    """Return whether recent developments are strong enough to influence the summary input."""
    if not items:
        return False
    for item in items:
        if item.get("score", -999) >= 6:
            return True
    return False


def fetch_recent_developments(
    market: str,
    stock_input: str,
    stock_name: str,
    stock_code: str,
    output_dir: str,
) -> tuple[list | None, str | None]:
    """Fetch recent stock-specific developments and save a markdown source."""
    analysis_dir = get_runtime_outputs_dir(market, stock_input)
    events_output_path = os.path.join(analysis_dir, "00_recent_developments.md")
    events_data_path = os.path.join(output_dir, "00_recent_developments.md")
    normalized_market = normalize_market_label(market)

    items = []

    try:
        if normalized_market == "CN":
            from download import CnInfoDownloader

            downloader = CnInfoDownloader()
            today = datetime.date.today()
            start = today - datetime.timedelta(days=120)
            announcements = downloader._query_announcements(
                {
                    "stock": [stock_code],
                    "category": [],
                    "searchkey": "",
                    "seDate": f"{start.isoformat()}~{today.isoformat()}",
                }
            )
            skip_markers = (
                "年度报告",
                "年报",
                "第一季度报告",
                "一季度报告",
                "半年度报告",
                "中期报告",
                "第三季度报告",
                "三季度报告",
                "摘要",
                "英文",
            )
            scored_items = []
            for ann in announcements:
                title = ann.get("announcementTitle", "")
                if not title or any(marker in title for marker in skip_markers):
                    continue
                url = ann.get("adjunctUrl")
                full_url = f"http://static.cninfo.com.cn/{url}" if url else None
                scored_items.append(
                    {
                        "score": _event_signal_score(title, normalized_market),
                        "date": _format_event_time(ann.get("announcementTime")),
                        "title": title,
                        "url": full_url,
                    }
                )
            scored_items.sort(key=lambda item: (item["score"], item["date"]), reverse=True)
            items = scored_items[:8]

        elif normalized_market == "US":
            from us_downloader import SecEdgarDownloader

            downloader = SecEdgarDownloader()
            cik = downloader.get_cik(stock_input)
            recent = downloader.get_filings(cik) if cik else None
            if recent:
                forms = recent.get("form", [])
                filing_dates = recent.get("filingDate", [])
                report_dates = recent.get("reportDate", [])
                docs = recent.get("primaryDocument", [])
                acc_nos = recent.get("accessionNumber", [])
                interesting_forms = {"8-K", "10-Q", "10-K", "6-K", "20-F"}
                scored_items = []
                for index, form in enumerate(forms):
                    if form not in interesting_forms:
                        continue
                    accession = acc_nos[index].replace("-", "") if index < len(acc_nos) else ""
                    doc = docs[index] if index < len(docs) else ""
                    filing_date = filing_dates[index] if index < len(filing_dates) else "N/A"
                    report_date = report_dates[index] if index < len(report_dates) else "N/A"
                    full_url = None
                    if cik and accession and doc:
                        full_url = (
                            f"https://www.sec.gov/Archives/edgar/data/"
                            f"{cik.lstrip('0')}/{accession}/{doc}"
                        )
                    title = f"{form} filed (report date: {report_date})"
                    scored_items.append(
                        {
                            "score": _event_signal_score(title, normalized_market),
                            "date": filing_date,
                            "title": title,
                            "url": full_url,
                        }
                    )
                scored_items.sort(key=lambda item: (item["score"], item["date"]), reverse=True)
                items = scored_items[:8]
    except Exception as exc:
        error_content = (
            "# 近期重大事件摘要抓取失败\n\n"
            f"- 公司: {stock_name}\n- 代码: {stock_code}\n- 市场: {normalized_market}\n- error: {exc}\n"
        )
        write_text(events_output_path, error_content)
        if os.path.abspath(events_output_path) != os.path.abspath(events_data_path):
            write_text(events_data_path, error_content)
        return None, events_data_path

    if items:
        lines = [
            "# 近期重大事件摘要",
            "",
            "以下内容用于在财报分析时补充最新事件背景，帮助判断估值、预期和结论是否需要动态调整。",
            "",
            f"- 公司: {stock_name}",
            f"- 股票代码: {stock_code}",
            f"- 市场: {normalized_market}",
            f"- 生成时间: {datetime.datetime.now().astimezone().isoformat()}",
            "",
            "## 近期事件",
            "",
        ]
        for idx, item in enumerate(items, start=1):
            lines.append(f"{idx}. {item['date']} | {item['title']}")
            if item.get("url"):
                lines.append(f"   - 链接: {item['url']}")
        lines.extend(["", "## 使用要求", "", "- 分析时必须结合这些近期事件判断预期变化、潜在风险和估值敏感点。"])
        content = "\n".join(lines) + "\n"
    else:
        content = (
            "# 近期重大事件摘要\n\n"
            f"- 公司: {stock_name}\n"
            f"- 股票代码: {stock_code}\n"
            f"- 市场: {normalized_market}\n\n"
            "当前未抓取到结构化的近期重大事件，请在结论中明确说明事件信息可能不完整。\n"
        )

    write_text(events_output_path, content)
    write_text(events_data_path, content)
    return items, events_data_path


def build_directional_summary_prompt(
    stock_name: str,
    is_bank_stock_profile: bool,
    include_recent_developments: bool = False,
) -> str:
    """Build a minimal stock-specific summary prompt and leave structure to the system prompt."""
    if is_bank_stock_profile:
        if include_recent_developments:
            return f"请基于已上传的全部财报（重点结合近5年年报及最新定期报告）、最新市场数据快照以及近期重大事件摘要，为{stock_name}写一份全方位的银行股投资总结。"
        return f"请基于已上传的全部财报（重点结合近5年年报及最新定期报告）和最新市场数据快照，为{stock_name}写一份全方位的银行股投资总结。"
    if include_recent_developments:
        return f"请基于已上传的全部财报（重点结合近5年年报及最新定期报告）、最新市场数据快照以及近期重大事件摘要，为{stock_name}写一份全方位的投资总结。"
    return f"请基于已上传的全部财报（重点结合近5年年报及最新定期报告）和最新市场数据快照，为{stock_name}写一份全方位的投资总结。"


def run_post_upload_analysis(
    notebook_id: str,
    market: str,
    stock_name: str,
    stock_input: str,
    market_snapshot: dict = None,
    include_recent_developments: bool = False,
    is_bank_stock_profile: bool = False,
):
    """Use NotebookLM to produce one directional summary plus a final report."""
    from upload import (
        ask_notebook_question,
        download_report,
        generate_report,
        get_conversation_history,
        list_artifacts,
    )

    analysis_dir = get_runtime_outputs_dir(market, stock_input)

    print(f"\n🧠 Running post-upload analysis for {stock_name}...")

    stage_started = log_stage_start("Directional summary")
    summary_prompt = build_directional_summary_prompt(
        stock_name,
        is_bank_stock_profile,
        include_recent_developments=include_recent_developments,
    )
    summary_ok, summary_output = ask_notebook_question(notebook_id, summary_prompt, new_conversation=True)
    summary_path = os.path.join(analysis_dir, "01_notebook_summary.md")
    if summary_ok:
        content = (
            f"# {stock_name} 方位总结\n\n"
            f"## 提问\n\n{summary_prompt}\n\n"
            f"## 回答\n\n{summary_output}\n"
        )
    else:
        fallback_path = prepare_manual_summary_fallback(
            analysis_dir=analysis_dir,
            notebook_id=notebook_id,
            stock_name=stock_name,
            summary_prompt=summary_prompt,
            summary_output=summary_output,
        )
        content = (
            f"# {stock_name} 方位总结\n\n"
            "## 自动调用状态\n\n"
            "本次 NotebookLM 自动总结失败，已切换到手动兜底流程。\n\n"
            f"- 手动兜底说明: {fallback_path}\n"
            f"- Notebook 链接: {get_notebook_url(notebook_id)}\n\n"
            "## 提示词\n\n"
            f"```text\n{summary_prompt}\n```\n\n"
            "## 自动调用失败信息\n\n"
            f"```text\n{summary_output}\n```\n"
        )
    write_text(summary_path, content)
    print(f"   📝 Saved notebook summary: {summary_path}")
    log_stage_end("Directional summary", stage_started)

    if not summary_ok:
        print("\n📋 Manual summary prompt:")
        print(summary_prompt)
        print(f"🔗 NotebookLM URL: {get_notebook_url(notebook_id)}")
        print("   ⚠️ NotebookLM summary failed; copied prompt to clipboard and opened the notebook for manual continuation.")
        print("   ⏭️ Skipping automated report generation because manual continuation is required.")
        return

    if market_snapshot:
        from market_data import snapshot_to_markdown

        snapshot_output_path = os.path.join(analysis_dir, "00_latest_market_snapshot.md")
        write_text(snapshot_output_path, snapshot_to_markdown(market_snapshot))
        print(f"   📊 Saved latest market snapshot: {snapshot_output_path}")

    report_stage = log_stage_start("NotebookLM report generation")
    report_prompt = build_report_prompt(
        stock_name,
        is_bank_stock_profile,
        include_recent_developments=include_recent_developments,
    )
    report_ok, report_output, artifact_id = generate_report(notebook_id, description=report_prompt)
    report_meta_path = os.path.join(analysis_dir, "99_report_artifact.json")
    write_text(report_meta_path, report_output)

    if report_ok:
        report_md_path = os.path.join(analysis_dir, "99_notebooklm_report.md")
        downloaded, download_output = download_report(notebook_id, report_md_path, artifact_id=artifact_id)
        if downloaded:
            print(f"   📄 Saved NotebookLM report: {report_md_path}")
        else:
            error_path = os.path.join(analysis_dir, "99_report_download_error.txt")
            write_text(error_path, download_output)
            print(f"   ⚠️ Report generated but download failed: {error_path}")
    else:
        print("   ⚠️ Report generation failed; raw artifact output has been saved.")
    log_stage_end("NotebookLM report generation", report_stage)

    artifact_stage = log_stage_start("Notebook artifact listing")
    artifacts_ok, artifacts, artifacts_output = list_artifacts(notebook_id)
    artifacts_path = os.path.join(analysis_dir, "98_notebook_artifacts.json")
    if artifacts_ok:
        write_text(
            artifacts_path,
            json.dumps(
                {
                    "notebook_id": notebook_id,
                    "artifact_count": len(artifacts),
                    "artifacts": artifacts,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
        )
    else:
        write_text(artifacts_path, artifacts_output)
    print(f"   🧾 Saved notebook artifact listing: {artifacts_path}")
    log_stage_end("Notebook artifact listing", artifact_stage)

    history_stage = log_stage_start("Notebook history fetch")
    history_ok, history_output = get_conversation_history(notebook_id, limit=20)
    history_path = os.path.join(analysis_dir, "98_conversation_history.txt")
    if history_ok:
        write_text(history_path, history_output)
    else:
        write_text(history_path, f"History fetch failed:\n\n{history_output}")
    print(f"   💬 Saved notebook conversation history: {history_path}")
    log_stage_end("Notebook history fetch", history_stage)

    write_text(os.path.join(analysis_dir, "LATEST_NOTEBOOK_ID.txt"), notebook_id + "\n")
    print(f"   📁 Analysis outputs directory: {analysis_dir}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python run.py <ticker_or_code_or_name>")
        sys.exit(1)

    cli_args = sys.argv[1:]
    stock_input = cli_args[0]
    require_summary_confirmation = "--confirm-summary-inputs" in cli_args[1:]
    market = detect_market(stock_input)
    
    # Use a persistent directory based on stock_input to cache downloads
    # This avoids re-downloading if a previous run failed during upload
    os.makedirs(os.path.join(runtime_root, "data"), exist_ok=True)
    os.makedirs(os.path.join(runtime_root, "outputs"), exist_ok=True)
    output_dir = get_runtime_data_dir(market, stock_input)
    
    all_files = []
    stock_name = stock_input
    stock_code = stock_input
    prompt_file = "financial_analyst_prompt.md"
    analysis_profile = "general"
    cn_report_plan = None

    print(f"🔍 Detected Market: {market}")
    
    # Check if we already have files in the cache
    if os.path.exists(output_dir) and os.listdir(output_dir):
        cache_stage = log_stage_start("Cache scan")
        print(f"📦 Found existing reports in cache: {output_dir}")
        all_files = [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith(".md") or f.endswith(".pdf")]
        all_files = filter_cached_files(market, all_files)

        if market.startswith("CN"):
            from download import CnInfoDownloader

            downloader = CnInfoDownloader()
            resolved_stock_code, stock_info = downloader.find_stock(stock_input)
            if resolved_stock_code:
                stock_code = resolved_stock_code
                stock_name = stock_info.get("zwjc", stock_code)
                cn_report_plan = downloader.build_report_plan()
                missing_reports = get_missing_cn_reports(all_files, cn_report_plan)
                if missing_reports:
                    print(
                        "⚠️ Cached A-share reports are missing the latest expected set: "
                        + ", ".join(missing_reports)
                    )
                    print("🔄 Refreshing A-share downloads to avoid using stale reports...")
                    all_files = []
        elif market == "HK" and hk_cache_needs_refresh(all_files):
            print(
                "⚠️ Cached HK reports look incomplete "
                f"({len(all_files)} file(s)); refreshing downloads..."
            )
            all_files = []
        log_stage_end("Cache scan", cache_stage)
    
    # If no files found, proceed to download
    if not all_files:
        download_stage = log_stage_start("Report download")
        download_failures = []
        os.makedirs(output_dir, exist_ok=True)
        if market == "US":
            from us_downloader import SecEdgarDownloader
            downloader = SecEdgarDownloader()
            all_files = downloader.get_reports(stock_input, output_dir)
            prompt_file = "us_financial_analyst_prompt.md"
            stock_name = stock_input.upper()
            stock_code = stock_input.upper()
        elif market == "HK":
            from hk_downloader import HkexDownloader
            downloader = HkexDownloader()
            reps = downloader.find_reports(stock_input)
            all_files = downloader.download_and_convert(reps, output_dir)
            stock_name = f"HK_{stock_input}"
            stock_code = stock_input
        else:
            from download import CnInfoDownloader
            downloader = CnInfoDownloader()
            resolved_stock_code, stock_info = downloader.find_stock(stock_input)
            if resolved_stock_code:
                stock_code = resolved_stock_code
                stock_name = stock_info.get("zwjc", stock_code)
                cn_report_plan = cn_report_plan or downloader.build_report_plan()
                annual_years = cn_report_plan["annual_years"]
                periodic_targets = cn_report_plan["periodic_targets"]
                print(
                    f"📅 A-share report plan ({cn_report_plan['as_of']}): "
                    f"annual={annual_years}, periodic={periodic_targets}"
                )
                all_files = downloader.download_annual_reports(stock_code, annual_years, output_dir)
                periodic = downloader.download_periodic_reports(stock_code, periodic_targets, output_dir)
                all_files.extend(periodic)
                download_failures = getattr(downloader, "failed_reports", [])
            else:
                print(f"❌ Stock not found: {stock_input}")
                if not os.listdir(output_dir):
                    os.rmdir(output_dir)
                sys.exit(1)
        log_stage_end("Report download", download_stage)
        if download_failures:
            print(f"⚠️ Download/convert failures: {len(download_failures)}")
            for item in download_failures[:10]:
                print(
                    f"   - [{item.get('stage', 'unknown')}] {item.get('title', 'unknown')} "
                    f"-> {item.get('path', 'N/A')}"
                )
            sync_download_failures_to_outputs(market, stock_input, download_failures)
    else:
        cache_resolve_stage = log_stage_start("Cache metadata resolve")
        # Determine stock_name for existing cache
        if market == "US":
            prompt_file = "us_financial_analyst_prompt.md"
            stock_name = stock_input.upper()
        elif market == "HK":
            stock_name = f"HK_{stock_input}"
            stock_code = stock_input
        else:
            from download import CnInfoDownloader
            downloader = CnInfoDownloader()
            resolved_stock_code, stock_info = downloader.find_stock(stock_input)
            if resolved_stock_code:
                stock_code = resolved_stock_code
                stock_name = stock_info.get("zwjc", stock_code)
        log_stage_end("Cache metadata resolve", cache_resolve_stage)

    if not all_files:
        print("❌ No reports downloaded")
        if os.path.exists(output_dir) and not os.listdir(output_dir):
            os.rmdir(output_dir)
        sys.exit(1)

    print(f"\n✅ Processed {len(all_files)} reports")

    is_bank_stock_profile, bank_reasons = detect_bank_stock(stock_input, stock_name, all_files)
    prompt_file, analysis_profile = get_analysis_assets(market, is_bank_stock_profile)
    if is_bank_stock_profile:
        print(f"🏦 Bank-stock profile detected; switching to bank-specific analysis prompt ({'; '.join(bank_reasons)})")

    snapshot_stage = log_stage_start("Market snapshot")
    market_snapshot = None
    market_snapshot_path = None
    market_snapshot, market_snapshot_path = fetch_market_snapshot(
        market=market,
        stock_input=stock_input,
        stock_name=stock_name,
        stock_code=stock_code,
        output_dir=output_dir,
    )
    if market_snapshot_path:
        all_files.append(market_snapshot_path)
    log_stage_end("Market snapshot", snapshot_stage)

    developments_stage = log_stage_start("Recent developments")
    recent_developments, recent_developments_path = fetch_recent_developments(
        market=market,
        stock_input=stock_input,
        stock_name=stock_name,
        stock_code=stock_code,
        output_dir=output_dir,
    )
    include_recent_developments = has_material_recent_developments(recent_developments)
    if include_recent_developments and recent_developments_path:
        all_files.append(recent_developments_path)
        print(f"📰 Saved recent developments: {recent_developments_path}")
    else:
        print("📰 No clearly material recent developments detected; skipping event context for summary.")
    log_stage_end("Recent developments", developments_stage)

    preview_path = write_summary_input_preview(
        market=market,
        stock_input=stock_input,
        stock_name=stock_name,
        stock_code=stock_code,
        is_bank_stock_profile=is_bank_stock_profile,
        market_snapshot=market_snapshot,
        recent_developments=recent_developments,
        include_recent_developments=include_recent_developments,
    )
    print(f"🧾 Saved summary input preview: {preview_path}")
    if require_summary_confirmation:
        print("⏸️ Pausing before NotebookLM summary because --confirm-summary-inputs was provided.")
        print("   Confirm the preview file, then rerun without --confirm-summary-inputs to continue.")
        return

    all_files = dedupe_file_paths(all_files)

    # Upload to NotebookLM
    from upload import (
        create_notebook,
        get_existing_source_map,
        list_notebooks,
        list_sources,
        normalize_source_name,
        remove_matching_sources,
        rename_notebook,
        upload_all_sources,
        configure_notebook,
        cleanup_temp_files,
        wait_for_sources,
    )
    
    notebook_state = load_notebook_state(output_dir)
    notebook_title = format_notebook_title(market, stock_code, stock_name)
    notebook_id = notebook_state.get("notebook_id")
    existing_source_map = {}
    notebook_title_map = {}

    if notebook_id:
        state_ok, existing_source_map, notebook_title_map = resolve_existing_notebook(
            notebook_id,
            notebook_state,
        )
        if state_ok:
            print(f"🔁 Reusing existing notebook: {notebook_id}")
            current_title = notebook_title_map.get(notebook_id, notebook_state.get("notebook_title", ""))
            if title_needs_rename(current_title, notebook_title):
                renamed_ok, rename_output = rename_notebook(notebook_id, notebook_title)
                if renamed_ok:
                    print(f"🏷️ Renamed legacy notebook title: {current_title} -> {notebook_title}")
                else:
                    print(f"⚠️ Failed to rename notebook title: {rename_output}")
        else:
            print("⚠️ Saved notebook could not be reused; creating a new notebook...")
            notebook_id = None
            existing_source_map = {}

    if not notebook_id:
        notebook_id = create_notebook(notebook_title)
    
    if notebook_id:
        notebook_stage = log_stage_start("Notebook setup")
        # Select prompt
        prompt_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "assets", prompt_file)
        configure_notebook(notebook_id, prompt_path)

        if market_snapshot_path:
            removed_ok, removed_ids = remove_matching_sources(
                notebook_id,
                [os.path.basename(market_snapshot_path)],
            )
            if removed_ok and removed_ids:
                print(f"🧹 Replaced previous market snapshot source(s): {len(removed_ids)}")
        if include_recent_developments and recent_developments_path:
            removed_ok, removed_ids = remove_matching_sources(
                notebook_id,
                [os.path.basename(recent_developments_path)],
            )
            if removed_ok and removed_ids:
                print(f"🧹 Replaced previous recent developments source(s): {len(removed_ids)}")

        source_map_ok, existing_source_map = get_existing_source_map(notebook_id)
        if not source_map_ok:
            existing_source_map = {}
        log_stage_end("Notebook setup", notebook_stage)

        files_to_upload = []
        for file_path in all_files:
            normalized_name = normalize_source_name(os.path.basename(file_path))
            is_market_snapshot = market_snapshot_path and os.path.abspath(file_path) == os.path.abspath(market_snapshot_path)
            is_recent_developments = (
                include_recent_developments
                and
                recent_developments_path
                and os.path.abspath(file_path) == os.path.abspath(recent_developments_path)
            )

            if is_market_snapshot or is_recent_developments or normalized_name not in existing_source_map:
                files_to_upload.append(file_path)
            else:
                print(f"↪️ Source already exists in notebook, skipping upload: {os.path.basename(file_path)}")

        upload_results = {"success": [], "failed": [], "source_ids": []}
        if files_to_upload:
            upload_stage = log_stage_start("Source upload")
            upload_results = upload_all_sources(notebook_id, files_to_upload)
            log_stage_end("Source upload", upload_stage)

            if upload_results.get("failed"):
                print("❌ Some new sources failed to upload; skipping re-analysis to avoid incomplete data.")
                sys.exit(1)

            if upload_results.get("source_ids"):
                print("\n⏳ Waiting for NotebookLM to finish processing sources...")
                wait_stage = log_stage_start("Source processing wait")
                wait_for_sources(notebook_id, upload_results["source_ids"], timeout=600)
                log_stage_end("Source processing wait", wait_stage)
        else:
            print("📚 No new financial report sources to upload; reusing existing notebook sources for re-analysis.")

        outputs_need_refresh, refresh_reasons = analysis_outputs_need_refresh(
            get_runtime_outputs_dir(market, stock_input)
        )
        should_run_analysis = bool(files_to_upload) or outputs_need_refresh

        if should_run_analysis:
            if files_to_upload:
                print("🧠 New or refreshed sources detected; regenerating NotebookLM summary and report.")
            else:
                print(
                    "🧠 Financial reports are already up to date, but saved analysis artifacts need refresh: "
                    + ", ".join(refresh_reasons)
                )
            analysis_stage = log_stage_start("Post-upload analysis")
            run_post_upload_analysis(
                notebook_id,
                market,
                stock_name,
                stock_input,
                market_snapshot=market_snapshot,
                include_recent_developments=include_recent_developments,
                is_bank_stock_profile=is_bank_stock_profile,
            )
            log_stage_end("Post-upload analysis", analysis_stage)
        else:
            print("✅ Financial reports and NotebookLM summary/report are already up to date; skipping re-analysis.")

        source_list_stage = log_stage_start("Final source listing")
        listed_ok, listed_sources = list_sources(notebook_id)
        log_stage_end("Final source listing", source_list_stage)
        notebook_state = {
            "notebook_id": notebook_id,
            "notebook_title": notebook_title,
            "market": market,
            "market_display": normalize_market_label(market),
            "stock_input": stock_input,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "analysis_profile": analysis_profile,
            "updated_at": datetime.datetime.now().astimezone().isoformat(),
            "source_count": len(listed_sources) if listed_ok else None,
            "sources": listed_sources if listed_ok else notebook_state.get("sources", []),
        }
        save_notebook_state(output_dir, notebook_state)
        
        print(f"\n🎉 COMPLETE! Notebook ID: {notebook_id}")
        print(f"📚 Notebook Title: {notebook_title}")
        cleanup_stage = log_stage_start("Cleanup")
        try:
            cleanup_temp_files(all_files, output_dir)
        except Exception as e:
            print(f"⚠️ Cleanup failed but will not block exit: {e}")
        finally:
            log_stage_end("Cleanup", cleanup_stage)

if __name__ == "__main__":
    main()
