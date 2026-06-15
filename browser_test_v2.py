from playwright.sync_api import sync_playwright
import sys

URL = "http://127.0.0.1:8000"
errors = []

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    # Capture ALL console messages
    page.on("console", lambda msg: (
        errors.append(f"CONSOLE [{msg.type}]: {msg.text}")
        if msg.type in ("error", "warning") else None
    ))

    # Capture unhandled JS errors
    page.on("pageerror", lambda err: errors.append(f"UNHANDLED: {err.message}"))

    page.goto(URL, timeout=15000)
    page.wait_for_load_state("networkidle", timeout=10000)

    # Check elements
    for sel, name in [("#loginBtn", "Login button"), ("#login-page", "Login page"), ("#app", "App")]:
        try:
            if page.locator(sel).count() > 0:
                print(f"  {name}: OK")
            else:
                errors.append(f"{name} MISSING")
        except Exception as e:
            errors.append(f"{name}: {e}")

    # Try clicking login button
    try:
        page.locator("#loginBtn").click(timeout=3000)
        page.wait_for_timeout(1000)
        print("  Click test: OK")
    except Exception as e:
        errors.append(f"Click failed: {e}")

    # Try actual login
    try:
        page.locator("#loginUser").fill("vv")
        page.locator("#loginPwd").fill("123456")
        page.locator("#loginBtn").click()
        page.wait_for_timeout(3000)
        # Check if login error appeared
        err_el = page.locator("#loginErr")
        if err_el.count() > 0:
            err_text = err_el.text_content()
            print(f"  Login response: {err_text[:100]}")
    except Exception as e:
        errors.append(f"Login test: {e}")

    page.screenshot(path="/tmp/mira_screenshot.png")

    browser.close()

if errors:
    print(f"\n{len(errors)} ISSUES:")
    for e in errors[:20]:
        print(f"  {e}")
else:
    print("\nALL CLEAN - no errors")
