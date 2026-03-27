#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Upload PDF files to NotebookLM using notebooklm-py CLI

Prerequisites:
  pip install notebooklm-py playwright
  playwright install chromium
  notebooklm login  # Authenticate first
"""

import sys
import os
import subprocess
import json
import shutil
import re
import time
import asyncio

# Ensure virtual environment's bin is in PATH
venv_bin = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".venv", "bin")
if os.path.exists(venv_bin):
    os.environ["PATH"] = venv_bin + os.pathsep + os.environ.get("PATH", "")

NOTEBOOKLM_BIN = os.path.join(venv_bin, "notebooklm")
SUMMARY_FALLBACK_TIMEOUT = 120.0

def check_notebooklm_installed() -> bool:
    """Check if notebooklm CLI is installed"""
    if os.path.exists(NOTEBOOKLM_BIN):
        return True
    return shutil.which("notebooklm") is not None


def run_notebooklm_command(args: list, timeout: int = 120) -> tuple:
    """Run notebooklm command and return (success, output)"""
    cmd = [NOTEBOOKLM_BIN] if os.path.exists(NOTEBOOKLM_BIN) else ["notebooklm"]
    started_at = time.time()
    command_text = " ".join(cmd + args)

    try:
        result = subprocess.run(
            cmd + args, capture_output=True, text=True, timeout=timeout
        )
        elapsed = time.time() - started_at
        output = result.stdout + result.stderr
        diag = f"[command] {command_text}\n[elapsed] {elapsed:.2f}s\n"
        return result.returncode == 0, diag + output
    except subprocess.TimeoutExpired as e:
        elapsed = time.time() - started_at
        partial_output = (e.stdout or "") + (e.stderr or "")
        return (
            False,
            (
                f"[command] {command_text}\n"
                f"[elapsed] {elapsed:.2f}s\n"
                f"[timeout] {timeout}s\n"
                "NotebookLM command timed out.\n\n"
                f"{partial_output}"
            ),
        )
    except Exception as e:
        elapsed = time.time() - started_at
        return False, (
            f"[command] {command_text}\n"
            f"[elapsed] {elapsed:.2f}s\n"
            f"[error] {e}"
        )


def extract_uuid(text: str) -> str:
    """Extract the first UUID from command output."""
    match = re.search(r"[a-f0-9-]{36}", text or "")
    return match.group(0) if match else None


def extract_json_object(text: str):
    """Extract the outermost JSON object from command output."""
    if not text:
        return None
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return None


def normalize_source_name(name: str) -> str:
    """Normalize source title/file names for comparison."""
    if not name:
        return ""
    base = os.path.basename(name.strip()).lower()
    for suffix in (".md", ".pdf", ".txt", ".docx", ".html"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return base


def create_notebook(title: str) -> str:
    """Create a new NotebookLM notebook, returns notebook ID or None"""
    print(f"📚 Creating notebook: {title}")

    success, output = run_notebooklm_command(["create", title])

    if not success:
        print(f"❌ Failed to create notebook: {output}", file=sys.stderr)
        return None

    # Parse output to find notebook ID
    # Output format: "Created notebook: <title> (ID: <id>)" or similar
    for line in output.split("\n"):
        if "ID:" in line or "id:" in line:
            notebook_id = extract_uuid(line)
            if notebook_id:
                print(f"✅ Created notebook: {notebook_id}")
                return notebook_id
        notebook_id = extract_uuid(line)
        if notebook_id:
            print(f"✅ Created notebook: {notebook_id}")
            return notebook_id

    # Fallback: return trimmed output
    print(f"⚠️ Output: {output}")
    return output.strip().split()[-1] if output.strip() else None


def list_notebooks() -> tuple[bool, list]:
    """List all notebooks in the current account."""
    success, output = run_notebooklm_command(["list", "--json"])
    if not success:
        return False, []

    data = extract_json_object(output)
    if not data:
        return False, []
    return True, data.get("notebooks", [])


def rename_notebook(notebook_id: str, new_title: str) -> tuple[bool, str]:
    """Rename one notebook."""
    return run_notebooklm_command(["rename", "--notebook", notebook_id, new_title])


def upload_source(notebook_id: str, file_path: str) -> tuple[bool, str]:
    """Upload a file as source to a notebook and return source ID when possible."""
    filename = os.path.basename(file_path)
    print(f"📤 Uploading: {filename}")

    # Set notebook context first
    success, output = run_notebooklm_command(["use", notebook_id])
    if not success:
        print(f"❌ Failed to set notebook: {output}", file=sys.stderr)
        return False, None

    last_output = ""
    for attempt in range(1, 4):
        success, output = run_notebooklm_command(["source", "add", file_path])
        last_output = output
        if success:
            print(f"   ✅ Uploaded successfully")
            source_id = extract_uuid(output)
            if source_id:
                print(f"   🆔 Source ID: {source_id}")
            return True, source_id
        print(f"   ⚠️ Upload attempt {attempt}/3 failed", file=sys.stderr)
        if attempt < 3:
            time.sleep(5)

    print(f"   ❌ Failed: {last_output}", file=sys.stderr)
    return False, None


def upload_all_sources(notebook_id: str, files: list) -> dict:
    """Upload multiple files to a notebook"""
    results = {"success": [], "failed": [], "source_ids": []}

    for file_path in files:
        ok, source_id = upload_source(notebook_id, file_path)
        if ok:
            results["success"].append(file_path)
            if source_id:
                results["source_ids"].append({"file": file_path, "source_id": source_id})
        else:
            results["failed"].append(file_path)

    return results


def list_sources(notebook_id: str) -> tuple[bool, list]:
    """List all sources in a notebook as structured data."""
    try:
        from notebooklm.client import NotebookLMClient
    except Exception:
        NotebookLMClient = None

    if NotebookLMClient is not None:
        async def _list_sources() -> tuple[bool, list]:
            async with await NotebookLMClient.from_storage(timeout=SUMMARY_FALLBACK_TIMEOUT) as client:
                sources = await client.sources.list(notebook_id)
                rows = []
                for index, source in enumerate(sources, start=1):
                    rows.append(
                        {
                            "index": index,
                            "id": source.id,
                            "title": source.title,
                            "type": str(source.kind),
                            "url": source.url,
                            "status": str(source.status),
                            "status_id": int(source.status) if source.status is not None else None,
                            "created_at": source.created_at.isoformat() if source.created_at else None,
                        }
                    )
                return True, rows

        try:
            return asyncio.run(_list_sources())
        except Exception:
            pass

    success, output = run_notebooklm_command(["source", "list", "--notebook", notebook_id, "--json"])
    data = extract_json_object(output) if success else None
    if data:
        return True, data.get("sources", [])
    return False, []


def delete_source(notebook_id: str, source_id: str) -> tuple[bool, str]:
    """Delete one source from a notebook."""
    return run_notebooklm_command(["source", "delete", source_id, "--notebook", notebook_id, "--yes"])


def get_existing_source_map(notebook_id: str) -> tuple[bool, dict]:
    """Build a lookup map for existing notebook sources by normalized title."""
    success, sources = list_sources(notebook_id)
    if not success:
        return False, {}

    source_map = {}
    for source in sources:
        normalized = normalize_source_name(source.get("title", ""))
        if not normalized:
            continue
        source_map.setdefault(normalized, []).append(source)
    return True, source_map


def remove_matching_sources(notebook_id: str, match_names: list[str]) -> tuple[bool, list]:
    """Delete notebook sources whose normalized titles match any provided name."""
    ok, source_map = get_existing_source_map(notebook_id)
    if not ok:
        return False, []

    deleted = []
    wanted = {normalize_source_name(name) for name in match_names if name}

    for normalized in wanted:
        for source in source_map.get(normalized, []):
            source_id = source.get("id")
            if not source_id:
                continue
            success, _ = delete_source(notebook_id, source_id)
            if success:
                deleted.append(source_id)

    return True, deleted


def wait_for_sources(notebook_id: str, source_ids: list, timeout: int = 300) -> dict:
    """Wait for uploaded sources to finish processing."""
    results = {"ready": [], "failed": []}

    for item in source_ids:
        source_id = item["source_id"] if isinstance(item, dict) else item
        success, output = run_notebooklm_command(
            ["source", "wait", source_id, "--notebook", notebook_id, "--timeout", str(timeout), "--json"]
        )
        if success:
            results["ready"].append({"source_id": source_id, "output": output})
            print(f"   ✅ Source ready: {source_id}")
        else:
            results["failed"].append({"source_id": source_id, "output": output})
            print(f"   ⚠️ Source not ready: {source_id}", file=sys.stderr)

    return results


def get_notebook_summary(notebook_id: str, include_topics: bool = True) -> tuple[bool, str]:
    """Fetch AI summary from NotebookLM."""
    args = ["summary", "--notebook", notebook_id]
    if include_topics:
        args.append("--topics")
    success, output = run_notebooklm_command(args)
    if success and "No summary available" not in output:
        return success, output

    should_fallback = (
        "Connection timed out calling SUMMARIZE" in output
        or "No summary available" in output
    )
    if not should_fallback:
        return success, output

    try:
        from notebooklm.client import NotebookLMClient
    except Exception as e:
        return False, output + f"\n[fallback_error] Failed to import notebooklm client: {e}"

    async def _fetch_summary() -> tuple[bool, str]:
        async with await NotebookLMClient.from_storage(timeout=SUMMARY_FALLBACK_TIMEOUT) as client:
            description = await client.notebooks.get_description(notebook_id)
            parts = []
            if description.summary:
                parts.append("Summary:\n" + description.summary)
            if include_topics and description.suggested_topics:
                topic_lines = ["", "Suggested Topics:"]
                for index, topic in enumerate(description.suggested_topics, start=1):
                    topic_lines.append(f"{index}. {topic.question}")
                parts.append("\n".join(topic_lines))
            if not parts:
                return True, "No summary available"
            return True, "\n\n".join(parts)

    try:
        return asyncio.run(_fetch_summary())
    except Exception as e:
        return False, output + f"\n[fallback_error] Python API summary fallback failed: {e}"


def ask_notebook_question(notebook_id: str, question: str, new_conversation: bool = True) -> tuple[bool, str]:
    """Ask one question and return the raw answer."""
    args = ["ask", "--notebook", notebook_id]
    if new_conversation:
        args.append("--new")
    args.append(question)
    success, output = run_notebooklm_command(args)
    if success:
        return success, output

    should_fallback = any(
        marker in output
        for marker in (
            "Chat request failed",
            "Server disconnected without sending a response",
            "Connection timed out",
        )
    )
    if not should_fallback:
        return success, output

    try:
        from notebooklm.client import NotebookLMClient
    except Exception as e:
        return False, output + f"\n[fallback_error] Failed to import notebooklm client: {e}"

    async def _ask_via_python_api() -> tuple[bool, str]:
        async with await NotebookLMClient.from_storage(timeout=SUMMARY_FALLBACK_TIMEOUT) as client:
            result = await client.chat.ask(notebook_id, question)
            return True, result.answer

    try:
        return asyncio.run(_ask_via_python_api())
    except Exception as e:
        return False, output + f"\n[fallback_error] Python API ask fallback failed: {e}"


def list_artifacts(notebook_id: str, artifact_type: str = "all") -> tuple[bool, list, str]:
    """List notebook artifacts and return structured data when available."""
    args = ["artifact", "list", "--notebook", notebook_id, "--json"]
    if artifact_type and artifact_type != "all":
        args.extend(["--type", artifact_type])

    success, output = run_notebooklm_command(args)
    if not success:
        return False, [], output

    data = extract_json_object(output)
    if not data:
        return False, [], output
    return True, data.get("artifacts", []), output


def get_conversation_history(notebook_id: str, limit: int = 20) -> tuple[bool, str]:
    """Fetch recent NotebookLM conversation history as raw text."""
    args = ["history", "--notebook", notebook_id, "--limit", str(limit)]
    return run_notebooklm_command(args)


def generate_report(notebook_id: str, description: str = None, report_format: str = "briefing-doc") -> tuple:
    """Generate a NotebookLM report artifact."""
    args = ["generate", "report", "--notebook", notebook_id, "--format", report_format, "--wait", "--json"]
    if description:
        args.append(description)
    success, output = run_notebooklm_command(args, timeout=900)
    artifact_id = extract_uuid(output)
    return success, output, artifact_id


def download_report(notebook_id: str, output_path: str, artifact_id: str = None) -> tuple[bool, str]:
    """Download the latest or specified report as markdown."""
    args = ["download", "report", output_path, "--notebook", notebook_id, "--force"]
    if artifact_id:
        args.extend(["--artifact", artifact_id])
    else:
        args.append("--latest")
    return run_notebooklm_command(args)


def cleanup_temp_files(files: list, temp_dir: str = None):
    """Remove temporary files after upload"""
    for f in files:
        try:
            os.remove(f)
        except Exception:
            pass

    if temp_dir and (temp_dir.startswith("/var/folders") or "/tmp/" in temp_dir):
        try:
            shutil.rmtree(temp_dir)
            print(f"🧹 Cleaned up temp directory: {temp_dir}")
        except Exception:
            pass


def configure_notebook(notebook_id: str, prompt_file: str) -> bool:
    """Configure notebook with custom prompt"""
    if not os.path.exists(prompt_file):
        print(f"⚠️ Prompt file not found: {prompt_file}")
        return False

    try:
        with open(prompt_file, "r", encoding="utf-8") as f:
            prompt = f.read()
    except Exception as e:
        print(f"❌ Error reading prompt file: {e}")
        return False

    print(f"⚙️ Configuring notebook with custom prompt...")
    # --persona takes TEXT, so we pass the content directly
    # We also set mode to 'detailed' and response-length to 'longer' for depth
    last_output = ""
    for attempt in range(1, 4):
        success, output = run_notebooklm_command(
            [
                "configure",
                "--notebook",
                notebook_id,
                "--persona",
                prompt,
                "--response-length",
                "longer",
            ]
        )
        last_output = output
        if success:
            print(f"   ✅ Configuration successful")
            return True
        print(f"   ⚠️ Configure attempt {attempt}/3 failed", file=sys.stderr)
        if attempt < 3:
            time.sleep(5)

    print(f"   ❌ Configuration failed: {last_output}", file=sys.stderr)
    return False


def main():
    """Main entry point"""
    if len(sys.argv) < 3:
        print("Usage: python upload.py <notebook_title> <pdf_file1> [pdf_file2] ...")
        print("       python upload.py <notebook_title> --json <json_file>")
        print("")
        print("The JSON file should contain output from download.py")
        sys.exit(1)

    # Check notebooklm is installed
    if not check_notebooklm_installed():
        print("❌ NotebookLM CLI not found!", file=sys.stderr)
        print("Install with: pip install notebooklm-py playwright")
        print("Then: playwright install chromium")
        print("Then authenticate with: notebooklm login")
        sys.exit(1)

    notebook_title = sys.argv[1]

    # Handle JSON input from download.py
    if sys.argv[2] == "--json":
        json_file = sys.argv[3]
        with open(json_file, "r") as f:
            data = json.load(f)
        files = data.get("files", [])
        temp_dir = data.get("output_dir")
        notebook_title = f"{data.get('stock_name', notebook_title)} 财务报告"
    else:
        files = sys.argv[2:]
        temp_dir = None

    if not files:
        print("❌ No files to upload", file=sys.stderr)
        sys.exit(1)

    print(f"📁 Files to upload: {len(files)}")

    # Create notebook
    notebook_id = create_notebook(notebook_title)
    if not notebook_id:
        sys.exit(1)

    # Upload all files
    results = upload_all_sources(notebook_id, files)

    # Summary
    print(f"\n{'=' * 50}")
    print(f"✅ Uploaded: {len(results['success'])} files")
    if results["failed"]:
        print(f"❌ Failed: {len(results['failed'])} files")
    print(f"📚 Notebook: {notebook_title}")
    print(f"🆔 ID: {notebook_id}")

    # Cleanup temp files
    if temp_dir:
        cleanup_temp_files(files, temp_dir)

    # Output JSON result
    result = {
        "notebook_id": notebook_id,
        "notebook_title": notebook_title,
        "uploaded": len(results["success"]),
        "failed": len(results["failed"]),
    }
    print("\n---JSON_OUTPUT---")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
