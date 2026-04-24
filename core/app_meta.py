import os

APP_VERSION = "3.2.1"
DEFAULT_ALLOWED_ORIGINS = ["https://shouhu.asia"]


def get_allowed_origins() -> list[str]:
    raw = os.environ.get("CORS_ALLOW_ORIGINS", "")
    if raw:
        origins = [item.strip() for item in raw.split(",") if item.strip()]
        if origins:
            return origins
    return DEFAULT_ALLOWED_ORIGINS.copy()
