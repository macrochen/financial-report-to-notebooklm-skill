#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Download A-share stock reports from cninfo.com.cn
Stores PDFs in temporary directory, outputs file paths for upload
"""

import sys
import os
import json
import tempfile
import datetime
import time
import random
import httpx
from converter import pdf_to_markdown

# Stock database location
STOCKS_JSON = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "assets", "stocks.json"
)


class CnInfoDownloader:
    """Downloads reports from cninfo.com.cn"""

    def __init__(self):
        self.cookies = {
            "JSESSIONID": "9A110350B0056BE0C4FDD8A627EF2868",
            "insert_cookie": "37836164",
        }
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:110.0) Gecko/20100101 Firefox/110.0",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "http://www.cninfo.com.cn",
            "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&lastPage=index",
        }
        self.timeout = httpx.Timeout(60.0)
        self.query_url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
        self.market_to_stocks = self._load_stocks()
        self.failed_reports = []

    def _load_stocks(self) -> dict:
        """Load stock database from JSON file"""
        if os.path.exists(STOCKS_JSON):
            with open(STOCKS_JSON, "r") as f:
                return json.load(f)
        return {}

    def find_stock(self, stock_input: str) -> tuple:
        """
        Find stock by code or name
        Returns: (stock_code, stock_info) or (None, None)
        """
        # Try as code first
        for market_stocks in self.market_to_stocks.values():
            if stock_input in market_stocks:
                return stock_input, market_stocks[stock_input]

        # Try as name
        for market_stocks in self.market_to_stocks.values():
            for code, info in market_stocks.items():
                if info.get("zwjc") == stock_input:
                    return code, info

        return None, None

    def build_report_plan(self, as_of: datetime.date = None, annual_report_count: int = 5) -> dict:
        """Build a dynamic A-share report download plan based on today's date."""
        as_of = as_of or datetime.date.today()

        latest_annual_year = as_of.year - 1
        annual_years = list(
            range(latest_annual_year - annual_report_count + 1, latest_annual_year + 1)
        )

        periodic_targets = {
            # Q1 filings are usually fully available after April.
            "q1": as_of.year if as_of >= datetime.date(as_of.year, 5, 1) else as_of.year - 1,
            # Semi-annual filings are usually fully available after August.
            "semi": as_of.year if as_of >= datetime.date(as_of.year, 9, 1) else as_of.year - 1,
            # Q3 filings are usually fully available after October.
            "q3": as_of.year if as_of >= datetime.date(as_of.year, 11, 1) else as_of.year - 1,
        }

        return {
            "as_of": as_of.isoformat(),
            "annual_years": annual_years,
            "periodic_targets": periodic_targets,
        }

    def _query_announcements(self, filter_params: dict) -> list:
        """Query cninfo API for announcements"""
        client = httpx.Client(
            headers=self.headers, cookies=self.cookies, timeout=self.timeout
        )

        # Get orgId for stock
        stock_code = filter_params["stock"][0]
        stock_info = None
        for market_stocks in self.market_to_stocks.values():
            if stock_code in market_stocks:
                stock_info = market_stocks[stock_code]
                break

        if not stock_info:
            return []

        payload = {
            "pageNum": 0,
            "pageSize": 30,
            "column": "szse",  # A-share market
            "tabName": "fulltext",
            "plate": "",
            "stock": f"{stock_code},{stock_info['orgId']}",
            "searchkey": filter_params.get("searchkey", ""),
            "secid": "",
            "category": ";".join(filter_params.get("category", [])),
            "trade": "",
            "seDate": filter_params.get("seDate", ""),
            "sortName": "",
            "sortType": "",
            "isHLtitle": False,
        }

        announcements = []
        has_more = True

        while has_more:
            payload["pageNum"] += 1
            try:
                resp = client.post(self.query_url, data=payload).json()
                has_more = resp.get("hasMore", False)
                if resp.get("announcements"):
                    announcements.extend(resp["announcements"])
            except Exception as e:
                print(f"Error querying API: {e}", file=sys.stderr)
                break

        return announcements

    def _download_pdf(self, announcement: dict, output_dir: str) -> str:
        """Download a single PDF file, returns file path"""
        client = httpx.Client(
            headers=self.headers, cookies=self.cookies, timeout=self.timeout
        )

        sec_code = announcement["secCode"]
        sec_name = announcement["secName"].replace("*", "s").replace("/", "-")
        title = announcement["announcementTitle"].replace("/", "-").replace("\\", "-")
        adjunct_url = announcement["adjunctUrl"]
        announcement_id = announcement["announcementId"]

        if announcement.get("adjunctType") != "PDF":
            return None

        filename = f"{sec_code}_{sec_name}_{title}_{announcement_id}.pdf"
        # Clean filename
        filename = "".join(c for c in filename if c.isalnum() or c in "._-")
        filepath = os.path.join(output_dir, filename)
        md_path = filepath.rsplit(".", 1)[0] + ".md"
        error_path = filepath.rsplit(".", 1)[0] + "_convert_error.txt"

        if os.path.exists(md_path):
            print(f"↪️ Reusing existing Markdown: {os.path.basename(md_path)}")
            return md_path

        if not os.path.exists(filepath):
            try:
                print(f"Downloading: {title}")
                resp = client.get(f"http://static.cninfo.com.cn/{adjunct_url}")
                resp.raise_for_status()
                with open(filepath, "wb") as f:
                    f.write(resp.content)
            except Exception as e:
                print(f"Download failed: {e}", file=sys.stderr)
                self.failed_reports.append(
                    {"title": title, "stage": "download", "error": str(e), "path": filepath}
                )
                return None

        converted_md_path, convert_error = pdf_to_markdown(filepath, timeout_seconds=90)
        if converted_md_path:
            if os.path.exists(error_path):
                os.remove(error_path)
            try:
                os.remove(filepath)  # Keep cache lean once markdown exists
            except Exception:
                pass
            return converted_md_path

        error_text = (
            f"PDF to Markdown conversion failed\n\n"
            f"title={title}\n"
            f"pdf_path={filepath}\n"
            f"error={convert_error or 'Unknown conversion error'}\n"
        )
        with open(error_path, "w", encoding="utf-8") as f:
            f.write(error_text)
        print(f"⚠️ Conversion failed, saved log: {os.path.basename(error_path)}")
        self.failed_reports.append(
            {"title": title, "stage": "convert", "error": convert_error or "Unknown conversion error", "path": error_path}
        )
        return None

    def _is_main_annual_report(self, title: str, year: int) -> bool:
        """Check if this is the main annual report (not summary/English)"""
        if f"{year}年年度报告" not in title and f"{year}年年报" not in title:
            return False
        if "摘要" in title or "英文" in title or "summary" in title.lower():
            return False
        if "更正" in title or "修订" in title:
            return False
        return True

    def _is_main_periodic_report(self, title: str, report_type: str) -> bool:
        """Check if this is a main periodic report"""
        if "摘要" in title or "英文" in title:
            return False
        if "更正" in title or "修订" in title:
            return False

        if report_type == "semi":
            return "半年度报告" in title or "中期报告" in title
        elif report_type == "q1":
            return "一季度" in title or "第一季度" in title
        elif report_type == "q3":
            return "三季度" in title or "第三季度" in title

        return False

    def download_annual_reports(
        self, stock_code: str, years: list, output_dir: str
    ) -> list:
        """Download annual reports for specified years"""
        downloaded = []

        for year in years:
            # Annual reports are published in the following year (March-April)
            search_start = f"{year + 1}-01-01"
            search_end = f"{year + 1}-06-30"

            filter_params = {
                "stock": [stock_code],
                "category": ["category_ndbg_szsh"],  # Annual reports
                "searchkey": f"{year}年年度报告",
                "seDate": f"{search_start}~{search_end}",
            }

            announcements = self._query_announcements(filter_params)

            for ann in announcements:
                if self._is_main_annual_report(ann["announcementTitle"], year):
                    filepath = self._download_pdf(ann, output_dir)
                    if filepath:
                        downloaded.append(filepath)
                        print(f"✅ Downloaded: {year} Annual Report")
                    break  # Only get one per year

        return downloaded

    def download_periodic_reports(
        self, stock_code: str, periodic_targets, output_dir: str
    ) -> list:
        """Download the latest available Q1, semi-annual, and Q3 reports."""
        downloaded = []

        if isinstance(periodic_targets, int):
            periodic_targets = {
                "q1": periodic_targets,
                "semi": periodic_targets,
                "q3": periodic_targets,
            }

        report_configs = [
            (
                "q1",
                "category_yjdbg_szsh",
                "一季度报告",
                periodic_targets.get("q1"),
                "04-01",
                "05-31",
            ),
            (
                "semi",
                "category_bndbg_szsh",
                "半年度报告",
                periodic_targets.get("semi"),
                "08-01",
                "09-30",
            ),
            (
                "q3",
                "category_sjdbg_szsh",
                "三季度报告",
                periodic_targets.get("q3"),
                "10-01",
                "11-30",
            ),
        ]

        for report_type, category, search_term, target_year, start_suffix, end_suffix in report_configs:
            if not target_year:
                continue

            filter_params = {
                "stock": [stock_code],
                "category": [category],
                "searchkey": search_term,
                "seDate": f"{target_year}-{start_suffix}~{target_year}-{end_suffix}",
            }

            announcements = self._query_announcements(filter_params)

            for ann in announcements:
                if self._is_main_periodic_report(ann["announcementTitle"], report_type):
                    filepath = self._download_pdf(ann, output_dir)
                    if filepath:
                        downloaded.append(filepath)
                        print(f"✅ Downloaded: {target_year} {search_term}")
                    break

        return downloaded


def main():
    """Main entry point - downloads reports and prints file paths"""
    if len(sys.argv) < 2:
        print("Usage: python download.py <stock_code_or_name> [output_dir]")
        print("Example: python download.py 600350")
        print("Example: python download.py 山东高速")
        sys.exit(1)

    stock_input = sys.argv[1]
    output_dir = (
        sys.argv[2] if len(sys.argv) > 2 else tempfile.mkdtemp(prefix="cninfo_reports_")
    )

    downloader = CnInfoDownloader()

    # Find stock
    stock_code, stock_info = downloader.find_stock(stock_input)
    if not stock_code:
        print(f"❌ Stock not found: {stock_input}", file=sys.stderr)
        sys.exit(1)

    stock_name = stock_info.get("zwjc", stock_code)
    print(f"📊 Found stock: {stock_code} ({stock_name})")
    print(f"📁 Output directory: {output_dir}")

    # Calculate years
    current_year = datetime.datetime.now().year
    annual_years = list(range(current_year - 5, current_year))  # Last 5 years

    print(f"\n📥 Downloading annual reports for: {annual_years}")
    annual_files = downloader.download_annual_reports(
        stock_code, annual_years, output_dir
    )

    # Try current year for periodic reports, fallback to previous year
    print(f"\n📥 Downloading periodic reports (Q1, semi-annual, Q3)...")
    periodic_files = downloader.download_periodic_reports(
        stock_code, current_year, output_dir
    )

    # If no periodic reports found in current year, try previous year
    if not periodic_files:
        print(f"   No {current_year} reports yet, trying {current_year - 1}...")
        periodic_files = downloader.download_periodic_reports(
            stock_code, current_year - 1, output_dir
        )
    # If some but not all, also check previous year for missing ones
    elif len(periodic_files) < 3:
        print(f"   Checking {current_year - 1} for additional reports...")
        prev_year_files = downloader.download_periodic_reports(
            stock_code, current_year - 1, output_dir
        )
        periodic_files.extend(prev_year_files)

    all_files = annual_files + periodic_files

    print(f"\n{'=' * 50}")
    print(f"✅ Downloaded {len(all_files)} reports")
    print(f"📁 Location: {output_dir}")
    print(f"\n📄 Files:")
    for f in all_files:
        print(f"  {os.path.basename(f)}")

    # Output JSON for easy parsing by upload script
    result = {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "output_dir": output_dir,
        "files": all_files,
    }

    # Write result to stdout marker for parsing
    print(f"\n---JSON_OUTPUT---")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
