from fastapi import APIRouter, Depends
from typing import Optional
from core.auth import get_current_user
from core.database import get_conn

router = APIRouter()

@router.get("")
def get_logs(
    act_id: Optional[str] = None,
    action_type: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    keyword: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
    user=Depends(get_current_user)
):
    conn = get_conn()
    where, params = ["1=1"], []
    if act_id:
        where.append("l.act_id=?"); params.append(act_id)
    if action_type:
        where.append("l.action_type=?"); params.append(action_type)
    if status:
        where.append("l.status=?"); params.append(status)
    if date_from:
        where.append("date(l.created_at)>=?"); params.append(date_from)
    if date_to:
        where.append("date(l.created_at)<=?"); params.append(date_to)
    if keyword:
        where.append("(l.target_name LIKE ? OR l.trigger_detail LIKE ? OR l.act_id LIKE ?)")
        kw = f"%{keyword}%"
        params.extend([kw, kw, kw])

    count_params = list(params)
    params.extend([limit, offset])

    rows = conn.execute(
        f"""SELECT l.*,
               COALESCE(a.name, l.act_id) as account_name,
               a.currency as account_currency,
               a.timezone as account_timezone
            FROM action_logs l
            LEFT JOIN accounts a ON a.act_id = l.act_id
            WHERE {' AND '.join(where)}
            ORDER BY l.created_at DESC LIMIT ? OFFSET ?""",
        params
    ).fetchall()

    total = conn.execute(
        f"""SELECT COUNT(*) as c FROM action_logs l
            LEFT JOIN accounts a ON a.act_id = l.act_id
            WHERE {' AND '.join(where)}""",
        count_params
    ).fetchone()["c"]

    conn.close()
    return {"total": total, "items": [dict(r) for r in rows]}
