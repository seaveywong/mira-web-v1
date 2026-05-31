from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.auth import get_current_user
from services.ad_ops import AdOpsError, set_adset_budget, set_status


router = APIRouter()


def _operator_name(user) -> str:
    if isinstance(user, dict):
        return user.get("username") or user.get("role") or "manual"
    return "manual"


class StatusRequest(BaseModel):
    act_id: str
    level: str
    target_id: str
    status: str
    target_name: Optional[str] = None


class BulkStatusRequest(BaseModel):
    items: List[StatusRequest]


class BudgetRequest(BaseModel):
    act_id: str
    adset_id: str
    daily_budget: Optional[float] = None
    daily_budget_usd: Optional[float] = None
    target_name: Optional[str] = None


@router.post("/status")
def update_status(body: StatusRequest, user=Depends(get_current_user)):
    try:
        return set_status(
            act_id=body.act_id,
            level=body.level,
            target_id=body.target_id,
            desired_status=body.status,
            target_name=body.target_name or "",
            operator=_operator_name(user),
        )
    except AdOpsError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/bulk-status")
def bulk_update_status(body: BulkStatusRequest, user=Depends(get_current_user)):
    if not body.items:
        raise HTTPException(status_code=400, detail="No items selected")
    if len(body.items) > 100:
        raise HTTPException(status_code=400, detail="Bulk operation supports at most 100 items")

    results = []
    success = 0
    failed = 0
    operator = _operator_name(user)
    for item in body.items:
        try:
            result = set_status(
                act_id=item.act_id,
                level=item.level,
                target_id=item.target_id,
                desired_status=item.status,
                target_name=item.target_name or "",
                operator=operator,
            )
            success += 1
            results.append({
                "ok": True,
                "act_id": item.act_id,
                "level": item.level,
                "target_id": item.target_id,
                "result": result,
            })
        except Exception as exc:
            failed += 1
            results.append({
                "ok": False,
                "act_id": item.act_id,
                "level": item.level,
                "target_id": item.target_id,
                "error": str(exc),
            })
    return {
        "status": "ok" if failed == 0 else "partial",
        "total": len(body.items),
        "success": success,
        "failed": failed,
        "results": results,
    }


@router.post("/adset-budget")
def update_adset_budget(body: BudgetRequest, user=Depends(get_current_user)):
    try:
        return set_adset_budget(
            act_id=body.act_id,
            adset_id=body.adset_id,
            daily_budget=body.daily_budget,
            daily_budget_usd=body.daily_budget_usd,
            target_name=body.target_name or "",
            operator=_operator_name(user),
        )
    except AdOpsError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
