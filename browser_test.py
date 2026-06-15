#!/usr/bin/env python3
"""Headless browser test: load page, check console, screenshot"""
from playwright.sync_api import sync_playwright
import sys, os

URL = os.environ.get("TEST_URL", "http://127.0.0.1:8000")
errors = []

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    # Capture console errors
    page.on("console", lambda msg: None)  # suppress normal logs
    page.on("pageerror", lambda err: errors.append(f"JS ERROR: {err.message}"))

    try:
        page.goto(URL, timeout=15000)
        page.wait_for_load_state("networkidle", timeout=10000)
    except Exception as e:
        errors.append(f"PAGE LOAD ERROR: {e}")

    # Take screenshot
    page.screenshot(path="/tmp/mira_screenshot.png", full_page=False)

    # Check key elements
    checks = {
        "Login button": "#loginBtn",
        "Login page": "#login-page",
        "App container": "#app",
    }
    for name, selector in checks.items():
        try:
            el = page.locator(selector)
            if el.count() > 0:
                print(f"  {name}: FOUND")
            else:
                errors.append(f"  {name}: MISSING ({selector})")
        except:
            errors.append(f"  {name}: ERROR checking")

    browser.close()

if errors:
    print(f"BROWSER TEST FAILED ({len(errors)} errors):")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)
else:
    print("BROWSER TEST PASSED")
