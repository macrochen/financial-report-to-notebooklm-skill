#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
US Stock Downloader (SEC EDGAR) - Fixed JSON Path
"""

import os
import sys
import json
import httpx
import time
import tempfile
from converter import html_to_markdown

class SecEdgarDownloader:
    def __init__(self):
        self.headers = {
            "User-Agent": "Institutional Research Agent (research@firm.com)",
            "Accept-Encoding": "gzip, deflate",
        }
        # Increased timeout and added HTTP2 support for better performance
        self.client = httpx.Client(timeout=60.0, follow_redirects=True, http2=True)
        self._ticker_mapping = None

    def _load_ticker_mapping(self):
        """Load and cache the official SEC ticker mapping."""
        if self._ticker_mapping is not None:
            return self._ticker_mapping

        resp = self.client.get("https://www.sec.gov/files/company_tickers.json", headers=self.headers)
        if resp.status_code != 200:
            raise RuntimeError(f"Ticker mapping fetch failed: HTTP {resp.status_code}")
        self._ticker_mapping = resp.json()
        return self._ticker_mapping

    def lookup_company_info(self, ticker: str) -> tuple[str | None, str | None]:
        """Return (CIK, company_name) from the SEC ticker mapping."""
        ticker = ticker.upper()
        ticker_norm = ticker.replace(".", "-")
        print(f"🔍 Looking up CIK for {ticker}...")
        try:
            data = self._load_ticker_mapping()
            for item in data.values():
                if item["ticker"] == ticker or item["ticker"] == ticker_norm:
                    cik = str(item["cik_str"]).zfill(10)
                    company_name = (item.get("title") or ticker).strip()
                    print(f"✅ Found CIK: {cik}")
                    return cik, company_name
            print("❌ CIK lookup failed: ticker not found in SEC mapping")
        except Exception as e:
            print(f"❌ CIK lookup error: {e}")
        return None, None

    def get_company_name(self, ticker: str) -> str | None:
        """Return the SEC company title for one ticker when available."""
        _, company_name = self.lookup_company_info(ticker)
        return company_name

    def get_cik(self, ticker: str) -> str:
        cik, _ = self.lookup_company_info(ticker)
        return cik

    def get_filings(self, cik: str):
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        print(f"🔍 Fetching filings for CIK {cik}...")
        try:
            resp = self.client.get(url, headers=self.headers)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("filings", {}).get("recent", {})
            print(f"❌ Filing fetch failed: HTTP {resp.status_code}")
        except Exception as e:
            print(f"❌ Filing fetch error: {e}")
        return None

    def download_with_retry(self, url: str, retries: int = 3) -> str:
        for i in range(retries):
            try:
                resp = self.client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    return resp.text
                elif resp.status_code == 429:
                    print(f"⚠️ Rate limited by SEC. Waiting...")
                    time.sleep(5 * (i + 1))
                else:
                    print(f"⚠️ Download failed with status {resp.status_code}")
            except Exception as e:
                print(f"⚠️ Attempt {i+1} failed: {e}")
                time.sleep(2)
        return None

    def download_filing(self, cik: str, accession_number: str, primary_document: str, output_dir: str, title: str) -> str:
        acc_no_clean = accession_number.replace("-", "")
        url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{acc_no_clean}/{primary_document}"
        
        md_filename = f"{title}.md"
        output_path = os.path.join(output_dir, md_filename)
        
        try:
            print(f"📥 Downloading: {title}")
            content = self.download_with_retry(url)
            if content:
                # Basic check if it's a real report (usually > 10KB for 10K/20F)
                if len(content) < 2000 and "6-K" not in title:
                    print(f"⚠️ Content too short ({len(content)} bytes), might be a placeholder.")
                
                return html_to_markdown(content, output_path)
        except Exception as e:
            print(f"❌ SEC Download error: {e}")
        return None

    def get_reports(self, ticker: str, output_dir: str) -> list:
        cik = self.get_cik(ticker)
        if not cik: return []
            
        recent = self.get_filings(cik)
        if not recent: return []
            
        forms = recent.get("form", [])
        acc_nos = recent.get("accessionNumber", [])
        docs = recent.get("primaryDocument", [])
        dates = recent.get("reportDate", [])
        
        results = []
        ten_k_count = 0
        six_k_count = 0
        
        print(f"📊 Analyzing {len(forms)} filings...")
        
        for i in range(len(forms)):
            form = forms[i]
            # Support both US (10-K/Q) and Foreign (20-F/6-K) issuers
            if form in ["10-K", "20-F"] and ten_k_count < 5:
                label = "10K" if form == "10-K" else "20F"
                res = self.download_filing(cik, acc_nos[i], docs[i], output_dir, f"{ticker}_{label}_{dates[i]}")
                if res:
                    results.append(res)
                    ten_k_count += 1
            elif form in ["10-Q", "6-K"] and six_k_count < 3:
                # For 6-K, we take the last 3 because they often contain quarterly results
                # instead of just picking one.
                label = "10Q" if form == "10-Q" else "6K"
                # Avoid duplicates for same date
                if any(f"{ticker}_{label}_{dates[i]}" in f for f in results):
                    continue
                    
                res = self.download_filing(cik, acc_nos[i], docs[i], output_dir, f"{ticker}_{label}_{dates[i]}")
                if res:
                    results.append(res)
                    if label == "10Q": six_k_count += 3 # Found a proper 10-Q, stop looking for 10Q
                    else: six_k_count += 1
            
            if ten_k_count >= 5 and six_k_count >= 3:
                break
            time.sleep(0.1) # Respect SEC rate limit (10/sec)
            
        return results

if __name__ == "__main__":
    ticker = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    downloader = SecEdgarDownloader()
    output = tempfile.mkdtemp()
    files = downloader.get_reports(ticker, output)
    print(f"DONE: {len(files)} files.")
