#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Format Converter: PDF/HTML to Markdown
"""

import os
import multiprocessing
import pymupdf4llm
import html2text
from bs4 import BeautifulSoup

def _pdf_to_markdown_worker(pdf_path: str, result_queue):
    """Run pymupdf4llm conversion in a child process so it can be timed out safely."""
    try:
        md_text = pymupdf4llm.to_markdown(pdf_path)
        result_queue.put({"ok": True, "markdown": md_text})
    except Exception as e:
        result_queue.put({"ok": False, "error": str(e)})


def pdf_to_markdown(pdf_path: str, timeout_seconds: int = 180) -> tuple[str | None, str | None]:
    """Convert PDF to Markdown with per-file timeout control."""
    md_path = pdf_path.rsplit(".", 1)[0] + ".md"
    print(f"📝 Converting to Markdown: {os.path.basename(pdf_path)}")

    ctx = multiprocessing.get_context("fork")
    result_queue = ctx.Queue()
    process = ctx.Process(
        target=_pdf_to_markdown_worker,
        args=(pdf_path, result_queue),
        daemon=True,
    )
    process.start()
    process.join(timeout_seconds)

    if process.is_alive():
        process.kill()
        process.join()
        return None, f"PDF to Markdown timed out after {timeout_seconds}s"

    if result_queue.empty():
        return None, "PDF to Markdown produced no result"

    result = result_queue.get()
    if result.get("ok"):
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(result["markdown"])
        return md_path, None

    error_text = result.get("error") or "Unknown PDF to Markdown error"
    print(f"❌ PDF to MD failed: {error_text}")
    return None, error_text


def html_to_markdown(html_content: str, output_path: str) -> str:
    """Convert HTML content to Markdown, returns md file path"""
    print(f"📝 Converting HTML to Markdown...")

    try:
        # Pre-process with BeautifulSoup to remove scripts/styles
        soup = BeautifulSoup(html_content, "lxml")
        for tag in soup(["script", "style"]):
            tag.decompose()
            
        h = html2text.HTML2Text()
        h.ignore_links = False
        h.ignore_images = True
        h.body_width = 0 # No wrapping
        
        md_text = h.handle(str(soup))
        
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(md_text)
        return output_path
    except Exception as e:
        print(f"❌ HTML to MD failed: {e}")
        return None
