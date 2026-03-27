# Global Financial Report to NotebookLM

自动从全球主要市场（A股、港股、美股）下载上市公司财报，并上传至 Google NotebookLM，利用 AI 驱动的“财务分析师”角色进行深度分析。首次运行会为公司创建专属 notebook，后续重跑默认复用同一 notebook 并增量补传新财报。

> 💡 **提示**: 本工具会自动为 NotebookLM 配置专业的“财务分析师”角色。普通公司默认沿用《手把手教你读财报》的排雷与三表验证方法；若自动识别为商业银行，则会切换到《手把手教你读财报2：18节课看透银行业》的银行股专用框架。

## ✨ 核心功能

- 🌍 **全球支持**: 
  - **A股**: 自动下载巨潮资讯 (cninfo) 近 5 年年报 + 当年定期报告。
  - **港股**: 自动抓取 HKEX 最新公告并转换为易读的 Markdown。
  - **美股**: 自动从 SEC EDGAR 获取最新的 10-K 和 10-Q 报表。
- 🤖 **AI 分析师**: 根据市场和行业自动植入专用 System Prompt。普通公司使用《手把手教你读财报》框架，商业银行会自动切换到银行股专用 Persona 与问题模板。
- 📦 **全自动流程**: 一键完成下载、最新市场数据抓取、笔记本创建或复用、角色配置、文件上传和上传后的结构化分析。
- 🔁 **增量续更**: 同一家公司重跑时默认复用历史 NotebookLM notebook，只补传新增财报，并基于累计 source 重新完整分析。
- 🏷️ **统一命名**: Notebook 标题统一采用 `[市场] 代码 名称 - 财报分析`，方便在 NotebookLM 中快速检索和排重。
- 📈 **最新市场快照**: 在分析前补充最新股价、总股本、市值等数据，并作为独立 source 上传给 NotebookLM。
- 🧠 **自动分析落盘**: 自动保存 Notebook Summary、主题问答结果和 NotebookLM 生成的 Markdown 研报。
- 🧹 **自动清理**: 上传完成后自动清理临时文件，保持系统整洁。
- 🔐 **稳定登录**: 使用 `notebooklm-py` 确保鉴权稳定可靠。

## 🚀 使用方法

### 安装步骤

1. **安装 Skill**
   在你的 Agent 终端中运行以下命令（或直接让 Agent 处理）：

   ```bash
   npx skills add jarodise/financial-report-to-notebookllm-skill
   ```

2. **安装依赖** (首次运行)
   进入目录并运行安装脚本：

     ```bash
     cd financial-report-to-notebookllm-skill && ./install.sh
     ```

3. **认证登录**
   如果你之前没用过 NotebookLM，请先登录：

   ```bash
   .venv/bin/notebooklm login
   ```

### 运行工具

你可以直接在终端运行工具：

```bash
# A股：按代码或名称
python3 scripts/run.py 600519
python3 scripts/run.py "贵州茅台"

# 美股：按 Ticker
python3 scripts/run.py TSLA

# 港股：按 5 位代码
python3 scripts/run.py 00700
```

运行完成后，可在 `outputs/<市场>_<输入>/` 查看结果，通常包括：

- `00_latest_market_snapshot.md`
- `01_notebook_summary.md`
- `02_*` 到 `07_*` 的主题问答分析
- `99_notebooklm_report.md`
- `LATEST_NOTEBOOK_ID.txt`

同一家公司后续重跑时，还会在 `data/<市场>_<输入>/notebook_state.json` 记录 notebook 状态，用于复用原有 notebook、跳过已上传财报，并替换旧的市场快照 source。
如果你已经有旧命名格式的 notebook，可运行 `scripts/rename_legacy_notebooks.py` 批量迁移到新的统一标题。

## 📂 项目结构

```
financial-report-to-notebookllm-skill/
├── data/               # notebook 复用状态（按公司保存）
├── package.json        # 项目元数据
├── SKILL.md            # LLM 指令和上下文说明
├── install.sh          # 依赖安装脚本
├── scripts/
│   ├── run.py          # 主流程控制脚本（市场识别 + 编排）
│   ├── download.py     # 巨潮资讯 (A股) 下载逻辑
│   ├── hk_downloader.py # HKEX (港股) 下载逻辑
│   ├── market_data.py   # 最新市场数据抓取与快照生成
│   ├── us_downloader.py # SEC (美股) 下载逻辑
│   └── upload.py       # NotebookLM 交互逻辑
└── assets/
    ├── financial_analyst_prompt.md      # A股/港股分析师提示词
    ├── bank_financial_analyst_prompt.md # 商业银行专用分析师提示词
    ├── us_financial_analyst_prompt.md   # 美股分析师提示词
    ├── analysis_questions_cn.json       # A股/港股自动分析问题模板
    ├── analysis_questions_bank.json     # 商业银行自动分析问题模板
    └── analysis_questions_us.json       # 美股自动分析问题模板
```

## ⚠️ 免责声明

本工具仅供教育和研究使用。请确保遵守各交易所信息披露平台和 Google NotebookLM 的服务条款。AI 提供的财务分析仅供参考，不构成任何投资建议。
