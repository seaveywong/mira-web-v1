from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.auth import get_current_user
from services.local_executor import (
    create_open_account_task,
    list_tasks,
    poll_task,
    update_task_from_node,
)


router = APIRouter()


class OpenAccountTaskRequest(BaseModel):
    act_id: str
    node_id: Optional[str] = ""


class PollRequest(BaseModel):
    node_id: str
    node_secret: str


class TaskUpdateRequest(BaseModel):
    node_id: str
    node_secret: str
    status: str
    progress: Optional[str] = ""
    result: Optional[dict] = None
    error: Optional[str] = ""
    screenshot_data_url: Optional[str] = ""


@router.get("/tasks")
def get_tasks(limit: int = 30, user=Depends(get_current_user)):
    return list_tasks(user, limit=limit)


@router.post("/tasks/open-account")
def create_open_account(body: OpenAccountTaskRequest, user=Depends(get_current_user)):
    task = create_open_account_task(user, body.act_id, body.node_id or None)
    return {"success": True, "task": task}


@router.post("/poll")
def poll(body: PollRequest):
    try:
        return {"success": True, **poll_task(body.node_id, body.node_secret)}
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/tasks/{task_id}/update")
def update_task(task_id: str, body: TaskUpdateRequest):
    try:
        task = update_task_from_node(
            task_id=task_id,
            node_id=body.node_id,
            node_secret=body.node_secret,
            status=body.status,
            progress=body.progress or "",
            result=body.result or {},
            error=body.error or "",
            screenshot_data_url=body.screenshot_data_url or "",
        )
        return {"success": True, "task": task}
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
