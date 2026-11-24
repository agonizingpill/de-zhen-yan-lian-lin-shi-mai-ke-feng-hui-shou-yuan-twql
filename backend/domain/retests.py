import json
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import (CheckConstraint, Column, DateTime, ForeignKey, Integer, JSON, String, Table,
                        UniqueConstraint, select, update)

from core.db import metadata, transaction
from core.queueing import QUEUE, broker

PENDING = "queued"
READY = "ready"
REJECTED = "rejected"

retests = Table(
    "reline_retests", metadata,
    Column("token", String(32), primary_key=True),
    Column("ticket", String(24), ForeignKey("channel_tickets.code"), nullable=False),
    Column("sound_window", String(24), ForeignKey("sound_windows.code"), nullable=False),
    Column("point", String(20), ForeignKey("microphone_points.code"), nullable=False),
    Column("channel", String(12), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("focus", JSON, nullable=False),
    Column("payload", JSON, nullable=False),
    Column("status", String(10), nullable=False, default=PENDING),
    Column("result", JSON),
    Column("detail", String(255)),
    Column("requested_by", String(80), nullable=False),
    Column("created_at", DateTime, nullable=False),
    Column("concluded_at", DateTime),
    UniqueConstraint("ticket", "sound_window", name="uq_reline_retest_ticket_window"),
    CheckConstraint("status IN ('queued','ready','rejected')"),
)


def public(row):
    data = {"token": row["token"], "ticket": row["ticket"], "window": row["sound_window"],
            "status": row["status"], "requested_by": row["requested_by"]}
    if row["status"] == READY:
        data["result"] = row["result"]
    elif row["status"] == REJECTED:
        data["detail"] = row["detail"]
    return data


def find(conn, ticket, window, lock=False):
    stmt = select(retests).where(retests.c.ticket == ticket, retests.c.sound_window == window)
    row = conn.execute(stmt.with_for_update() if lock else stmt).mappings().first()
    return dict(row) if row else None


def open_retest(conn, ticket, focus, payload, user):
    token = uuid4().hex
    conn.execute(retests.insert().values(
        token=token, ticket=ticket, sound_window=focus["code"], point=focus["point"],
        channel=focus["channel"], revision=focus["revision"], focus=focus, payload=payload,
        status=PENDING, requested_by=user["sub"], created_at=datetime.now(timezone.utc)))
    return find(conn, ticket, focus["code"])


def dispatch(row):
    """Queue the retest using the payload captured when it was submitted.

    Recovery replays the stored window snapshot, never a reline window added later.
    """
    item = {"token": row["token"], "owner": row["requested_by"], "input": row["payload"]}
    with broker.pipeline() as pipe:
        pipe.set(f'compute:{row["token"]}',
                 json.dumps({"owner": row["requested_by"], "status": PENDING}), ex=86400)
        pipe.rpush(QUEUE, json.dumps(item))
        pipe.execute()


def queued_tokens():
    """Tokens still waiting in the active queue.

    The ``:processing`` list is deliberately excluded: an entry left there by a crash must
    be treated as not-yet-resumed so recovery can re-enqueue it. A duplicate delivery is
    harmless because conclusions are recorded once (see :func:`conclude`).
    """
    tokens = set()
    for raw in broker.lrange(QUEUE, 0, -1):
        try:
            tokens.add(json.loads(raw)["token"])
        except (ValueError, KeyError, TypeError):
            continue
    return tokens


def recover():
    """Requeue retests left pending by an interrupted run, so a restart resumes them.

    The stored submission snapshot is replayed unchanged. Already queued or concluded
    retests are skipped, so recovery is safe to repeat.
    """
    pending = resume()
    queued = queued_tokens()
    resumed = []
    for row in pending:
        if row["token"] not in queued:
            dispatch(row)
            resumed.append(row["token"])
    return resumed


def by_token(conn, token):
    row = conn.execute(select(retests).where(retests.c.token == token)).mappings().first()
    return dict(row) if row else None


def status(token):
    with transaction() as conn:
        row = by_token(conn, token)
    return None if row is None else row["status"]


def ledger(conn, ticket=None):
    columns = (retests.c.token, retests.c.ticket, retests.c.sound_window, retests.c.status,
               retests.c.focus, retests.c.result, retests.c.detail, retests.c.requested_by,
               retests.c.created_at, retests.c.concluded_at)
    stmt = select(*columns)
    if ticket is not None:
        stmt = stmt.where(retests.c.ticket == ticket)
    stmt = stmt.order_by(retests.c.created_at.desc()).limit(160)
    return [dict(row) for row in conn.execute(stmt).mappings()]


def resume():
    """Retests left pending by an interrupted run, oldest first."""
    stmt = (select(retests).where(retests.c.status == PENDING)
            .order_by(retests.c.created_at.asc()))
    with transaction() as conn:
        return [dict(row) for row in conn.execute(stmt).mappings()]


def resume_one(row):
    """Requeue a single pending retest unless it is already waiting or already concluded."""
    fresh = status(row["token"])
    if fresh != PENDING or row["token"] in queued_tokens():
        return False
    dispatch(row)
    return True


def lookup(user, token):
    """Status of a durable reline retest, or None when the token is a plain computation.

    A retest is a shared desk work item: both duty officers (and reviewers) may follow the same
    token, so the second officer reconnecting to the original retest is not locked out. The
    read-only guest still cannot open another account's compute.
    """
    with transaction() as conn:
        row = by_token(conn, token)
    if row is None:
        return None
    if user["role"] == "viewer":
        raise HTTPException(403, detail="仅提交人和复核员可查看计算")
    return public(row)


def conclusion(token, ticket):
    with transaction() as conn:
        row = by_token(conn, token)
    if row is None:
        return None
    if row["ticket"] != ticket:
        raise HTTPException(409, detail="计算凭证不属于这张复线单")
    if row["status"] == PENDING:
        raise HTTPException(409, detail="等待复算完成后再提交")
    if row["status"] == REJECTED:
        raise HTTPException(409, detail=row["detail"] or "复线检测未通过")
    return row


def conclude(token, status, result=None, detail=None):
    """Record one conclusion for a retest.

    Returns True when this run recorded the conclusion, False when another run already did
    (a late duplicate must not replace the issued conclusion), None when the token is not a
    durable retest and belongs to the plain compute queue.
    """
    values = {"status": status, "concluded_at": datetime.now(timezone.utc)}
    if status == READY:
        values["result"] = result
    elif detail:
        values["detail"] = detail[:255]
    with transaction() as conn:
        if by_token(conn, token) is None:
            return None
        outcome = conn.execute(update(retests).where(retests.c.token == token,
                                                    retests.c.status == PENDING).values(**values))
    return outcome.rowcount == 1


def ledger(conn, ticket=None):
    columns = (retests.c.token, retests.c.ticket, retests.c.sound_window, retests.c.status,
               retests.c.focus, retests.c.result, retests.c.detail, retests.c.requested_by,
               retests.c.created_at, retests.c.concluded_at)
    stmt = select(*columns)
    if ticket is not None:
        stmt = stmt.where(retests.c.ticket == ticket)
    stmt = stmt.order_by(retests.c.created_at.desc()).limit(160)
    return [dict(row) for row in conn.execute(stmt).mappings()]
