from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.auth import get_current_user
from services.local_executor import (
    create_api_task,
    list_tasks,
    poll_task,
    poll_tasks,
    update_task_from_node,
)


router = APIRouter()


class ProbeAccountTaskRequest(BaseModel):
    act_id: str
    node_id: Optional[str] = ""


class UpdateStatusTaskRequest(BaseModel):
    act_id: str
    object_id: str
    level: str
    status: str = "PAUSED"
    node_id: Optional[str] = ""


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
    screenshot_data_url: Optional[str] = ""


@router.get("/tasks")
def get_tasks(limit: int = 30, user=Depends(get_current_user)):
    return list_tasks(user, limit=limit)


@router.post("/tasks/probe-account")
def create_probe_account(body: ProbeAccountTaskRequest, user=Depends(get_current_user)):
    task = create_api_task(
        user=user,
        task_type="graph_account_probe",
        act_id=body.act_id,
        params={},
        node_id=body.node_id or None,
    )
    return {"success": True, "task": task}


@router.post("/tasks/update-status")
def create_update_status(body: UpdateStatusTaskRequest, user=Depends(get_current_user)):
    task = create_api_task(
        user=user,
        task_type="graph_update_status",
        act_id=body.act_id,
        params={
            "object_id": body.object_id,
            "level": body.level,
            "status": body.status,
        },
        node_id=body.node_id or None,
    )
    return {"success": True, "task": task}


@router.post("/tasks/open-account")
def create_open_account_disabled():
    raise HTTPException(status_code=410, detail="本地执行器已切换为纯 API 模式，不再创建 UI 打开任务")


@router.post("/poll")
def poll(body: PollRequest):
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
def poll_task_batch(body: PollRequest):
    return poll(body)


@router.post("/tasks/{task_id}/update")
def update_task(task_id: str, body: TaskUpdateRequest):
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


@router.post("/tasks/{task_id}/result")
def complete_task(task_id: str, body: TaskUpdateRequest):
    return update_task(task_id, body)
