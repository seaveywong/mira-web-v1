from __future__ import annotations

import hashlib
import os
import tempfile
from typing import Optional
import zipfile

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from core.auth import ROLE_LEVELS, get_current_user, is_superadmin
from services.local_token_bridge import (
    create_registration,
    heartbeat_node,
    list_nodes,
    register_node,
    remove_node,
)


router = APIRouter()
MAX_EXTENSION_PACKAGE_BYTES = 30 * 1024 * 1024


class RegistrationRequest(BaseModel):
    node_name: Optional[str] = ""


class RegisterRequest(BaseModel):
    code: str
    node_name: Optional[str] = ""
    browser: Optional[str] = "Chrome"
    user_agent: Optional[str] = ""


class HeartbeatRequest(BaseModel):
    node_id: str
    node_secret: str
    access_token: Optional[str] = ""
    expires_at: Optional[str] = ""
    expires_in_minutes: Optional[int] = None
    token_summary: Optional[dict] = None
    node_name: Optional[str] = ""
    browser: Optional[str] = "Chrome"
    user_agent: Optional[str] = ""


def _require_operator(user):
    if ROLE_LEVELS.get((user or {}).get("role", "viewer"), 0) < ROLE_LEVELS["operator"]:
        raise HTTPException(status_code=403, detail="Operator permission required")


def _require_superadmin(user):
    if not is_superadmin(user):
        raise HTTPException(status_code=403, detail="Superadmin only")


def _downloads_dir() -> str:
    root = os.environ.get("MIRA_FRONTEND_DIR", "/opt/mira/frontend")
    return os.path.join(root, "downloads")


def _extension_path() -> str:
    return os.path.join(_downloads_dir(), "mira-local-api-executor.zip")


def _legacy_extension_path() -> str:
    return os.path.join(_downloads_dir(), "mira-local-token-bridge.zip")


def _extension_meta(path: str) -> dict:
    if not os.path.exists(path):
        return {"exists": False, "filename": os.path.basename(path)}
    stat = os.stat(path)
    sha = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            sha.update(chunk)
    return {
        "exists": True,
        "filename": os.path.basename(path),
        "size_bytes": stat.st_size,
        "updated_at_ts": stat.st_mtime,
        "sha256": sha.hexdigest(),
    }


def _validate_extension_zip(path: str) -> dict:
    if not zipfile.is_zipfile(path):
        raise HTTPException(status_code=400, detail="请上传 .zip 格式的 Chrome 插件包")
    try:
        with zipfile.ZipFile(path) as zf:
            names = [n.replace("\\", "/") for n in zf.namelist() if n and not n.endswith("/")]
            if not names:
                raise HTTPException(status_code=400, detail="插件包为空")
            if any(n.startswith("/") or ".." in n.split("/") for n in names):
                raise HTTPException(status_code=400, detail="插件包路径不安全")
            manifest_paths = [n for n in names if n.endswith("manifest.json")]
            if not manifest_paths:
                raise HTTPException(status_code=400, detail="插件包缺少 manifest.json")
            root_manifest = "manifest.json" in manifest_paths
            nested_manifest = any(n.count("/") == 1 and n.endswith("/manifest.json") for n in manifest_paths)
            if not root_manifest and not nested_manifest:
                raise HTTPException(status_code=400, detail="manifest.json 必须在根目录或一级插件目录内")
            return {"file_count": len(names), "manifest_path": "manifest.json" if root_manifest else manifest_paths[0]}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"插件包校验失败：{exc}")


@router.get("/nodes")
def get_nodes(user=Depends(get_current_user)):
    _require_operator(user)
    return list_nodes(user)


@router.post("/registration")
def create_registration_code(body: RegistrationRequest, user=Depends(get_current_user)):
    _require_operator(user)
    try:
        data = create_registration(user, body.node_name or "")
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    public_base_url = os.environ.get("MIRA_PUBLIC_BASE_URL", "https://shouhu.asia").rstrip("/")
    return {
        "success": True,
        "server_url": public_base_url,
        **data,
    }


@router.post("/register")
def register_local_node(body: RegisterRequest):
    try:
        return {"success": True, **register_node(
            code=body.code,
            node_name=body.node_name or "",
            browser=body.browser or "Chrome",
            user_agent=body.user_agent or "",
        )}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/heartbeat")
def receive_heartbeat(body: HeartbeatRequest):
    try:
        node = heartbeat_node(
            node_id=body.node_id,
            node_secret=body.node_secret,
            access_token=body.access_token or "",
            expires_at=body.expires_at or "",
            expires_in_minutes=body.expires_in_minutes,
            token_summary=body.token_summary or {},
            node_name=body.node_name or "",
            browser=body.browser or "Chrome",
            user_agent=body.user_agent or "",
        )
        return {"success": True, "node": node}
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete("/nodes/{node_id}")
def delete_node(node_id: str, user=Depends(get_current_user)):
    _require_operator(user)
    try:
        removed = remove_node(node_id, user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    if not removed:
        raise HTTPException(status_code=404, detail="本地 Token 节点不存在")
    return {"success": True}


@router.get("/extension.zip")
def download_extension(user=Depends(get_current_user)):
    _require_operator(user)
    path = _extension_path()
    if not os.path.exists(path):
        path = _legacy_extension_path()
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="插件包还没有生成，请联系管理员重新发布")
    return FileResponse(
        path,
        filename="mira-local-api-executor.zip",
        media_type="application/zip",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/extension-package")
def get_extension_package_meta(user=Depends(get_current_user)):
    _require_superadmin(user)
    return {
        "success": True,
        "package": _extension_meta(_extension_path()),
        "legacy": _extension_meta(_legacy_extension_path()),
        "download_name": "mira-local-api-executor.zip",
    }


@router.post("/extension-package")
async def upload_extension_package(file: UploadFile = File(...), user=Depends(get_current_user)):
    _require_superadmin(user)
    filename = os.path.basename(file.filename or "")
    if not filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="请上传 .zip 插件包；Chrome 扩展安装包不要直接上传 .rar")
    os.makedirs(_downloads_dir(), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix="mira-local-api-executor-", suffix=".zip")
    os.close(fd)
    size = 0
    try:
        with open(tmp_path, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_EXTENSION_PACKAGE_BYTES:
                    raise HTTPException(status_code=413, detail="插件包超过 30MB，请精简后再上传")
                out.write(chunk)
        validation = _validate_extension_zip(tmp_path)
        os.replace(tmp_path, _extension_path())
        legacy = _legacy_extension_path()
        if os.path.exists(legacy):
            os.remove(legacy)
        return {"success": True, "package": _extension_meta(_extension_path()), "validation": validation}
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        try:
            await file.close()
        except Exception:
            pass


@router.delete("/extension-package")
def delete_extension_package(user=Depends(get_current_user)):
    _require_superadmin(user)
    removed = []
    for path in (_extension_path(), _legacy_extension_path()):
        if os.path.exists(path):
            os.remove(path)
            removed.append(os.path.basename(path))
    return {"success": True, "removed": removed, "package": _extension_meta(_extension_path())}
