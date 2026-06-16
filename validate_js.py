#!/usr/bin/env python3
"""Validate JS syntax in frontend HTML before deploying."""
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

repo_html = Path(__file__).resolve().parent / "frontend" / "index.html"
html_file = Path(os.environ.get("MIRA_HTML_FILE") or (repo_html if repo_html.exists() else "/opt/mira/frontend/index.html"))
with html_file.open("r", encoding="utf-8") as f:
    html = f.read()

# Extract all inline <script> blocks (no src= attribute)
scripts = []
for m in re.finditer(r"<script\b([^>]*)>(.*?)</script>", html, re.DOTALL):
    attrs = m.group(1)
    js = m.group(2).strip()
    # Skip external scripts (with src=)
    if 'src=' in attrs:
        continue
    if js:
        scripts.append(js)

if not scripts:
    print("NO inline scripts found!")
    sys.exit(1)

errors = []
for i, js in enumerate(scripts):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(js)
        tmpfile = f.name

    result = subprocess.run(
        ["node", "--check", tmpfile],
        capture_output=True, text=True, timeout=15
    )
    os.unlink(tmpfile)

    if result.returncode != 0:
        err = result.stderr.strip()
        # Only show first error line
        first_line = err.split('\n')[0] if err else 'unknown error'
        errors.append(f"Block {i+1} ({len(js)} bytes): {first_line}")

if errors:
    print(f"JS SYNTAX ERRORS ({len(errors)}):")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)
else:
    print(f"JS VALID: {len(scripts)} blocks, 0 errors")
