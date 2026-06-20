#!/usr/bin/env python3
"""Smoke tests for Cloudflare Pages landing package generation.

These tests intentionally avoid Cloudflare network calls. They verify the
locally generated static package for the most fragile commercial path:
form links must redirect through the Worker rotation endpoint, not through
client-side-only JavaScript.
"""

import shutil
import sys
import tempfile
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    import requests  # noqa: F401
except ModuleNotFoundError:
    sys.modules["requests"] = types.SimpleNamespace(
        request=lambda *args, **kwargs: None,
        exceptions=types.SimpleNamespace(RequestException=Exception),
    )

from services.landing_publisher import prepare_template  # noqa: E402


def assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def make_template_dir() -> Path:
    tmp = Path(tempfile.mkdtemp(prefix="lp_tpl_test_"))
    (tmp / "landing.html").write_text(
        """
<!doctype html>
<html><head><script>
var LP_PIXEL_ID = "";
var LP_TARGET_URL = "";
</script></head><body><a href="#" id="cta">Open</a></body></html>
""".strip(),
        encoding="utf-8",
    )
    return tmp


def test_form_link_uses_worker_redirect():
    template_dir = make_template_dir()
    work_dir = None
    try:
        work_dir = Path(
            prepare_template(
                template_dir,
                pixel_id="123456",
                target_urls=["https://wa.me/111", "https://wa.me/222"],
                rotation_mode="sequential",
                link_kind="form",
                worker_enabled=True,
                tracking_enabled=True,
                protection_enabled=False,
                page_id=88,
                ingest_secret="secret",
                ingest_url="https://shouhu.asia/api/landing-pages/events/ingest",
            )
        )
        html = (work_dir / "index.html").read_text(encoding="utf-8")
        worker = (work_dir / "_worker.js").read_text(encoding="utf-8")

        assert_true((work_dir / "landing.html").exists(), "landing.html should be generated")
        assert_true((work_dir / "_worker.js").exists(), "form links must include a Worker")
        assert_true("/__edge/redirect" in html, "form page should point to Worker redirect endpoint")
        assert_true('"link_kind":"form"' in worker, "Worker config must preserve form link kind")
        assert_true('"rotation_mode":"sequential"' in worker, "Worker config must preserve rotation mode")
        assert_true("https://wa.me/111" in worker and "https://wa.me/222" in worker, "Worker must receive all target URLs")
        assert_true("shouhu.asia" in worker, "Worker keeps the configured public ingest host before deployment")
        forbidden_ip = ".".join(["43", "129", "230", "237"])
        assert_true(forbidden_ip not in worker, "Worker config must not expose the server IP")
        assert_true("url.pathname === '/_worker.js'" in worker, "Worker source route must not be publicly readable")
        assert_true("directFormRedirect" in worker, "Worker must handle root path as direct form redirect")
        assert_true("ad_slug" in worker, "Worker must preserve ad slug on redirect requests")
    finally:
        shutil.rmtree(template_dir, ignore_errors=True)
        if work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)


def test_normal_landing_page_keeps_template_and_targets():
    template_dir = make_template_dir()
    work_dir = None
    try:
        work_dir = Path(
            prepare_template(
                template_dir,
                pixel_id="pixel-1",
                target_urls=["https://example.com/a", "https://example.com/b"],
                rotation_mode="random",
                link_kind="landing",
                worker_enabled=False,
                tracking_enabled=False,
            )
        )
        html = (work_dir / "index.html").read_text(encoding="utf-8")
        assert_true(not (work_dir / "_worker.js").exists(), "plain landing page without tracking should not include Worker")
        assert_true('var LP_PIXEL_ID = "pixel-1";' in html, "pixel id should be injected")
        assert_true("LP_TARGET_URLS" in html, "client target rotation should be injected")
        assert_true("https://example.com/a" in html and "https://example.com/b" in html, "all targets should be present")
    finally:
        shutil.rmtree(template_dir, ignore_errors=True)
        if work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)


def test_worker_landing_preserves_ad_slug_for_button_redirect():
    template_dir = make_template_dir()
    work_dir = None
    try:
        work_dir = Path(
            prepare_template(
                template_dir,
                pixel_id="pixel-1",
                target_urls=["https://wa.me/111"],
                rotation_mode="sequential",
                link_kind="landing",
                worker_enabled=True,
                tracking_enabled=True,
                protection_enabled=False,
                page_id=99,
                ingest_secret="secret",
                ingest_url="https://shouhu.asia/api/landing-pages/events/ingest",
                route_url="https://shouhu.asia/api/landing-pages/router/next",
            )
        )
        html = (work_dir / "index.html").read_text(encoding="utf-8")
        worker = (work_dir / "_worker.js").read_text(encoding="utf-8")

        assert_true('var LP_TARGET_URL = "/__edge/redirect";' in html, "worker landing should point CTA to server redirect")
        assert_true("function withAdSlug" in html, "client tracker must decorate redirect links with the ad slug")
        assert_true("ad_slug" in html, "client tracker must append ad_slug to redirect URLs")
        assert_true("adSlugFromRequest" in worker, "Worker must recover slug from path, query, or referer")
        assert_true("url.searchParams.get('ad_slug')" in worker, "Worker must read slug from redirect query")
        assert_true("request.headers.get('referer')" in worker, "Worker must recover slug from referer as fallback")
    finally:
        shutil.rmtree(template_dir, ignore_errors=True)
        if work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    test_form_link_uses_worker_redirect()
    test_normal_landing_page_keeps_template_and_targets()
    test_worker_landing_preserves_ad_slug_for_button_redirect()
    print("landing_publisher smoke tests passed")

