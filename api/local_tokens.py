from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from core.auth import ROLE_LEVELS, get_current_user
from services.local_token_bridge import (
    create_registration,
    heartbeat_node,
    list_nodes,
    register_node,
    remove_node,
)


router = APIRouter()


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
    root = os.environ.get("MIRA_FRONTEND_DIR", "/opt/mira/frontend")
    path = os.path.join(root, "downloads", "mira-local-api-executor.zip")
    if not os.path.exists(path):
        path = os.path.join(root, "downloads", "mira-local-token-bridge.zip")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="插件包还没有生成，请联系管理员重新发布")
    return FileResponse(
        path,
        filename="mira-local-api-executor.zip",
        media_type="application/zip",
        headers={"Cache-Control": "no-store"},
    )
