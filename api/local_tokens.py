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
from services.local_executor import poll_task, poll_tasks, update_task_from_node


router = APIRouter()
MAX_EXTENSION_PACKAGE_BYTES = 30 * 1024 * 1024


class RegistrationRequest(BaseModel):
    node_name: Optional[str] = ""


class RegisterRequest(BaseModel):
    code: Optional[str] = ""
    bind_code: Optional[str] = ""
    install_id: Optional[str] = ""
    node_name: Optional[str] = ""
    browser_name: Optional[str] = ""
    browserName: Optional[str] = ""
    display_name: Optional[str] = ""
    displayName: Optional[str] = ""
    browser: Optional[str] = "Chrome"
    user_agent: Optional[str] = ""
    extension_version: Optional[str] = ""
    capabilities: Optional[list] = None


class HeartbeatRequest(BaseModel):
    node_id: str
    node_secret: str
    install_id: Optional[str] = ""
    access_token: Optional[str] = ""
    expires_at: Optional[str] = ""
    expires_in_minutes: Optional[int] = None
    token_summary: Optional[dict] = None
    node_name: Optional[str] = ""
    browser_name: Optional[str] = ""
    browserName: Optional[str] = ""
    display_name: Optional[str] = ""
    displayName: Optional[str] = ""
    browser: Optional[str] = "Chrome"
    user_agent: Optional[str] = ""
    extension_version: Optional[str] = ""
    capabilities: Optional[list] = None
    status: Optional[str] = ""
    fb_user: Optional[dict] = None
    accounts: Optional[list] = None
    ad_accounts: Optional[list] = None
    adAccounts: Optional[list] = None
    account_ids: Optional[list] = None
    accountIds: Optional[list] = None
    ad_account_ids: Optional[list] = None
    adAccountIds: Optional[list] = None
    queue: Optional[dict] = None
    last_error: Optional[str] = ""


class PollRequest(BaseModel):
    node_id: str
    node_secret: Optional[str] = ""
    capacity: Optional[int] = 1
    running_task_ids: Optional[list] = None


class TaskUpdateRequest(BaseModel):
    node_id: str
    node_secret: Optional[str] = ""
    status: str
    progress: Optional[str] = ""
    result: Optional[dict] = None
    data: Optional[dict] = None
    error: Optional[str] = ""
    duration_ms: Optional[int] = None


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
        node_name = (
            body.node_name
            or body.browser_name
            or body.browserName
            or body.display_name
            or body.displayName
            or ""
        )
        return {"success": True, **register_node(
            code=body.bind_code or body.code,
            node_name=node_name,
            browser=body.browser or "Chrome",
            user_agent=body.user_agent or "",
            install_id=body.install_id or "",
            extension_version=body.extension_version or "",
            capabilities=body.capabilities or [],
        )}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/bind")
def bind_local_node(body: RegisterRequest):
    return register_local_node(body)


@router.post("/heartbeat")
def receive_heartbeat(body: HeartbeatRequest):
    try:
        token_summary = dict(body.token_summary or {})
        accounts = (
            body.accounts
            or body.ad_accounts
            or body.adAccounts
            or token_summary.get("accounts")
            or token_summary.get("ad_accounts")
            or token_summary.get("adAccounts")
            or []
        )
        account_id_aliases = (
            body.account_ids
            or body.accountIds
            or body.ad_account_ids
            or body.adAccountIds
            or token_summary.get("account_ids")
            or token_summary.get("accountIds")
            or token_summary.get("ad_account_ids")
            or token_summary.get("adAccountIds")
            or []
        )
        if accounts is not None or account_id_aliases or body.fb_user is not None or body.capabilities is not None or body.last_error:
            account_ids = []
            writable_count = 0
            for item in accounts:
                if not isinstance(item, dict):
                    continue
                act_id = (
                    item.get("act_id")
                    or item.get("actId")
                    or item.get("account_id")
                    or item.get("accountId")
                    or item.get("ad_account_id")
                    or item.get("adAccountId")
                    or item.get("id")
                )
                if act_id:
                    account_ids.append(act_id)
                write_status = str(item.get("write_status") or item.get("status") or "").lower()
                if write_status in {"", "writable", "active", "ok", "可写"}:
                    writable_count += 1
            for item in account_id_aliases or []:
                if item:
                    account_ids.append(item)
            caps = set(body.capabilities or token_summary.get("capabilities") or [])
            fb_user = body.fb_user or {}
            token_summary.update({
                "present": bool(token_summary.get("present") or accounts or body.status == "online"),
                "fb_user_id": token_summary.get("fb_user_id") or fb_user.get("id") or "",
                "fb_user_name": token_summary.get("fb_user_name") or fb_user.get("name") or "",
                "account_ids": account_ids,
                "accounts": accounts,
                "has_ads_management": bool(
                    token_summary.get("has_ads_management")
                    or "ads_management" in caps
                    or "ad_write" in caps
                    or "graph_post" in caps
                    or writable_count > 0
                ),
                "has_ads_read": bool(
                    token_summary.get("has_ads_read")
                    or "ads_read" in caps
                    or "account_probe" in caps
                    or "graph_get" in caps
                    or accounts
                    or account_ids
                ),
                "permissions": token_summary.get("permissions") or {"granted": sorted(caps), "declined": []},
                "last_error": token_summary.get("last_error") or body.last_error or "",
                "capabilities": sorted(caps),
            })
        node = heartbeat_node(
            node_id=body.node_id,
            node_secret=body.node_secret,
            access_token=body.access_token or "",
            expires_at=body.expires_at or "",
            expires_in_minutes=body.expires_in_minutes,
            token_summary=token_summary,
            node_name=(
                body.node_name
                or body.browser_name
                or body.browserName
                or body.display_name
                or body.displayName
                or ""
            ),
            browser=body.browser or "Chrome",
            user_agent=body.user_agent or "",
            install_id=body.install_id or "",
            extension_version=body.extension_version or "",
            capabilities=body.capabilities or token_summary.get("capabilities") or [],
            runtime_status=body.status or "",
            reported_accounts=accounts or token_summary.get("accounts") or [],
            queue=body.queue or {},
        )
        return {"success": True, "node": node}
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/poll")
def poll_local_token_tasks(body: PollRequest):
    try:
        if int(body.capacity or 1) > 1:
            return {"success": True, **poll_tasks(
                body.node_id,
                body.node_secret or "",
                capacity=int(body.capacity or 1),
                running_task_ids=body.running_task_ids or [],
            )}
        return {"success": True, **poll_task(body.node_id, body.node_secret or "")}
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/tasks/poll")
def poll_local_token_task_batch(body: PollRequest):
    return poll_local_token_tasks(body)


@router.post("/tasks/{task_id}/update")
def update_local_token_task(task_id: str, body: TaskUpdateRequest):
    try:
        result = body.result if body.result is not None else {"data": body.data or {}}
        if body.duration_ms is not None and isinstance(result, dict):
            result = dict(result)
            result.setdefault("duration_ms", body.duration_ms)
        task = update_task_from_node(
            task_id=task_id,
            node_id=body.node_id,
            node_secret=body.node_secret or "",
            status=body.status,
            progress=body.progress or "",
            result=result or {},
            error=body.error or "",
            screenshot_data_url="",
        )
        return {"success": True, "task": task}
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/tasks/{task_id}/result")
def complete_local_token_task(task_id: str, body: TaskUpdateRequest):
    return update_local_token_task(task_id, body)


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
