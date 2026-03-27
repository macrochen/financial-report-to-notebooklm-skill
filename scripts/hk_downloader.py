#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HK Stock Downloader (HKEXnews) - Ultimate Fixed Version
Correctly clicks Search and captures results using multiple fallback methods.
"""

import os
import sys
import httpx
import tempfile
import time
import re
from playwright.sync_api import sync_playwright

class HkexDownloader:
    def __init__(self):
        self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        self.include_keywords = [
            "年報",
            "年报",
            "年度報告",
            "年度报告",
            "中期報告",
            "中期报告",
            "中期業績",
            "中期业绩",
            "中期報告書",
            "季度報告",
            "季度报告",
            "第一季度報告",
            "第三季度報告",
        ]
        self.exclude_keywords = [
            "esg",
            "環境、社會及管治",
            "环境、社会及管治",
            "可持續發展",
            "可持续发展",
            "sustainability",
            "sustainable",
            "governance",
            "摘要",
            "更正",
            "補充",
            "补充",
            "结果",
            "業績公告",
            "业绩公告",
            "公告",
            "通函",
        ]

    def is_financial_report_title(self, title: str) -> bool:
        """Keep annual/interim/quarterly reports and reject ESG/announcements."""
        normalized = (title or "").strip()
        normalized_lower = normalized.lower()
        if not normalized:
            return False
        if any(keyword.lower() in normalized_lower for keyword in self.exclude_keywords):
            return False
        return any(keyword.lower() in normalized_lower for keyword in self.include_keywords)

    def add_report(self, reports: list, title: str, full_url: str):
        """Add a report if it passes title filtering and is not duplicated."""
        if not self.is_financial_report_title(title):
            return
        if any(item["url"] == full_url for item in reports):
            return
        print(f"  ✅ 捕获: {title}")
        reports.append({"title": title, "url": full_url})

    def find_reports(self, stock_code: str) -> list:
        stock_code = stock_code.zfill(5)
        print(f"🚀 启动终极匹配模式: 抓取港股 {stock_code}...")
        
        reports = []
        url = "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=zh"
        
        with sync_playwright() as p:
            # Default to headless mode so the downloader can run in sandboxed
            # or CI-like environments where a headed browser is unavailable.
            browser = p.chromium.launch(headless=True, slow_mo=1000)
            context = browser.new_context(user_agent=self.user_agent, viewport={'width': 1280, 'height': 1000})
            page = context.new_page()
            
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                
                # 1. 第一步：录入代码并选中联想词
                print("1️⃣ 录入股份代号...")
                page.click("#searchStockCode")
                page.type("#searchStockCode", stock_code, delay=150)
                suggestion = page.locator(".autocomplete-suggestion").first
                suggestion.wait_for(timeout=10000)
                suggestion.click()
                print("✅ 已选中公司")

                # 2. 第二步：点击标标题类别触发框
                print("2️⃣ 点击‘標題類別’触发框...")
                page.click("#tier1-select .combobox-field")
                page.click(".combobox-boundlist .droplist-item[data-value='rbAfter2006']")

                # 3. 第三步：选择财务报表大类
                print("3️⃣ 展开‘財務報表’大类菜单...")
                page.click("#rbAfter2006 .combobox-field")
                time.sleep(1)
                page.click("li[data-value='40000']")
                print("✅ 已点击‘財務報表 / 環境、社會及管治資料’")

                # 4. 第四步：在下级菜单选择“所有”
                print("4️⃣ 正在勾选‘所有’...")
                try:
                    page.click("li[data-value='40000'] li[data-value='-2']", timeout=5000)
                except:
                    page.evaluate("document.querySelector('li[data-value=\"40000\"] li[data-value=\"-2\"]')?.click()")
                
                page.keyboard.press("Escape")
                time.sleep(1)

                # 5. 第五步：【关键点击】点击搜寻按钮
                print("5️⃣ 点击深蓝色‘搜尋’按钮...")
                # 补全之前缺失的代码行
                search_btn = page.locator(".filter__btn-applyFilters-js.btn-blue").first
                search_btn.click()

                # 6. 等待数据加载
                print("⏳ 正在等待数据加载 (10s)...")
                # 显式等待 URL 跳转或特定元素
                try:
                    page.wait_for_selector(".table-container, .doc-link", timeout=15000)
                except:
                    print("⚠️ 自动同步超时，执行硬等待...")
                
                time.sleep(5)
                print(f"📍 当前 URL: {page.url}")

                # 7. 提取链接 (双重保险)
                print("📋 正在提取符合条件的报表链接...")
                
                # 方法 A: 结果表逐行提取，优先使用整行文本做标题判断
                links = page.query_selector_all(".doc-link a, .table-container a[href*='.pdf']")
                print(f"🔎 扫描到 {len(links)} 个潜在链接...")

                for link_el in links:
                    try:
                        title = link_el.inner_text().strip()
                        href = link_el.get_attribute("href")
                        if not href or ".pdf" not in href.lower():
                            continue

                        row_text = ""
                        try:
                            row = link_el.locator("xpath=ancestor::tr[1]")
                            if row.count():
                                row_text = row.inner_text().strip()
                        except Exception:
                            row_text = ""

                        title = row_text or title
                        full_url = "https://www1.hkexnews.hk" + href if href.startswith("/") else href

                        self.add_report(reports, title, full_url)
                    except Exception:
                        continue
                    if len(reports) >= 12:
                        break

                # 方法 B: 源码正则提取 (降级兜底)
                if not reports:
                    print("🔥 触发降级方案：从页面源码提取标题和链接，并继续执行财报过滤...")
                    content = page.content()
                    pdf_matches = re.findall(
                        r'href="(/listedco/listconews/sehk/[^"]+\.pdf)"[^>]*>(.*?)</a>',
                        content,
                        re.IGNORECASE | re.DOTALL,
                    )
                    for pdf_url, raw_title in pdf_matches:
                        full_url = "https://www1.hkexnews.hk" + pdf_url
                        title = re.sub(r"<[^>]+>", " ", raw_title)
                        title = re.sub(r"\s+", " ", title).strip()
                        self.add_report(reports, title, full_url)

                print(f"🎉 最终获取到 {len(reports)} 份报表。")
                time.sleep(2)
                
            except Exception as e:
                print(f"❌ 流程异常: {e}")
                page.screenshot(path="hkex_final_crash.png")
            finally:
                browser.close()
                
        return reports

    def download_and_convert(self, reports: list, output_dir: str) -> list:
        results = []
        headers = {"User-Agent": self.user_agent, "Referer": "https://www1.hkexnews.hk/"}
        
        with httpx.Client(timeout=60.0, headers=headers, follow_redirects=True) as client:
            for r in reports:
                success = False
                for attempt in range(3):
                    try:
                        clean_title = "".join(c for c in r["title"] if c.isalnum() or c in " _-").strip()
                        filename = f"{clean_title}.pdf"
                        filepath = os.path.join(output_dir, filename)
                        
                        print(f"📥 下载 ({attempt+1}/3): {r['title']}")
                        resp = client.get(r["url"])
                        
                        if resp.status_code == 200:
                            # 港股长报转 Markdown 很慢，直接保留 PDF 给 NotebookLM 更稳。
                            if resp.content.startswith(b"%PDF"):
                                with open(filepath, "wb") as f:
                                    f.write(resp.content)
                                results.append(filepath)
                                success = True
                                break
                            else:
                                print(f"⚠️ 下载内容似乎不是有效的 PDF，重试中...")
                        else:
                            print(f"⚠️ 下载失败: HTTP {resp.status_code}")
                        
                        time.sleep(2) # 失败重试等待
                    except Exception as e:
                        print(f"❌ 失败: {e}")
                        time.sleep(2)
                
                if not success:
                    print(f"🚫 放弃下载: {r['title']}")
                time.sleep(1)
        return results

if __name__ == "__main__":
    downloader = HkexDownloader()
    downloader.find_reports("00700")
