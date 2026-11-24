from fastapi import HTTPException
from sqlalchemy import select
from core.audit import record
from core.db import metadata,transaction
from core.queueing import result
from domain import retests
from domain.catalog import calibration,latest,one,same_slot
from domain.interventions import findings,tickets
from domain.learning import relative_to
from domain.soundboard import points,windows
from domain.states import Finding,Patch,Ticket
from sqlalchemy import Column,Integer,Table

patch_mutex=Table("patch_serial",metadata,Column("code",Integer,primary_key=True))


def lock_patch(conn):
    conn.execute(select(patch_mutex).with_for_update()).first()


def isolate(form,user):
    with transaction() as conn:
        lock_patch(conn)
        note=one(conn,findings,form.finding)
        old=one(conn,windows,note["focus_window"])
        point=one(conn,points,old["point"],lock=True)
        if note["category"]!=Finding.LOCAL:
            raise HTTPException(409,"同频协作或正常结果必须保留，不能据此局部隔离")
        if point["patch"]!=Patch.LIVE or point["revision"]!=old["revision"]:
            raise HTTPException(409,"麦位已经隔离或发现属于旧接线")
        if latest(conn,point,"live")["code"]!=old["code"]:
            raise HTTPException(409,"旧诊断不能覆盖本麦位的新声检窗")
        if len(set(relative_to(old,same_slot(conn,old))))>=3:
            raise HTTPException(409,"当前同期窗口已形成同频协作，保留整条线路")
        occupied=conn.execute(select(points.c.code).where(points.c.channel==form.destination)).first()
        if form.destination==point["channel"] or occupied:
            raise HTTPException(409,"复线必须使用独立空闲的备用线路")
        conn.execute(tickets.insert().values(code=form.code,point=point["code"],finding=form.finding,
                     old_channel=point["channel"],new_channel=form.destination,state=Ticket.ISOLATED,isolated_by=user["sub"]))
        conn.execute(points.update().where(points.c.code==point["code"]).values(
            patch=Patch.ISOLATED,channel=form.destination,revision=point["revision"]+1))
        record(conn,user,f"isolate:{form.code}:{form.destination}")
        return one(conn,tickets,form.code)


def ensure_reline(conn,ticket,focus):
    point=one(conn,points,ticket["point"])
    if ticket["state"]!=Ticket.ISOLATED or point["patch"]!=Patch.ISOLATED:
        raise HTTPException(409,"线路已复位或不在隔离阶段")
    old=one(conn,windows,one(conn,findings,ticket["finding"])["focus_window"])
    if (focus["point"]!=point["code"] or focus["channel"]!=ticket["new_channel"]
        or focus["revision"]!=point["revision"] or focus["purpose"]!="reline" or focus["slot"]<=old["slot"]):
        raise HTTPException(409,"必须使用同一隔离麦位、备用线路及后续时刻的重新声检窗口")
    if latest(conn,point,"reline")["code"]!=focus["code"]:
        raise HTTPException(409,"存在更新的复线窗口，请使用最新窗口")


def probe(code,form,user):
    with transaction() as conn:
        ticket=one(conn,tickets,code,lock=True)
        if ticket["state"]!=Ticket.ISOLATED:
            raise HTTPException(409,"线路已复位或不在隔离阶段")
        focus=one(conn,windows,form.window)
        existing=retests.find(conn,code,focus["code"],lock=True)
        if existing is not None:
            row,created=existing,False
        else:
            ensure_reline(conn,ticket,focus)
            payload={"kind":"reline","ticket":code,"focus":focus,
                     "baseline":calibration(conn),"simultaneous":[]}
            row,created=retests.open_retest(conn,code,focus,payload,user),True
            record(conn,user,f"reline-probe:{code}:{focus['code']}")
    if row["status"]==retests.PENDING:
        retests.resume_one(row)
    return {"token":row["token"],"status":row["status"],"window":row["sound_window"],
            "ticket":row["ticket"],"resumed":not created}


def restore(code,form,user):
    durable=retests.conclusion(form.token,code)
    if durable is None:
        data=result(user,form.token)
        payload,rating=data["input"],data["result"]
        if payload["kind"]!="reline" or payload["ticket"]!=code:
            raise HTTPException(409,"计算凭证不属于这张复线单")
        focus=payload["focus"]
    else:
        focus,rating=durable["focus"],durable["result"]
    with transaction() as conn:
        lock_patch(conn)
        ticket=one(conn,tickets,code,lock=True)
        point=one(conn,points,ticket["point"],lock=True)
        if ticket["state"]==Ticket.RESTORED:
            if ticket["reline_window"]==focus["code"]:
                return one(conn,tickets,code)
            raise HTTPException(409,"该隔离单已按其他复线窗复位，请查看原复位记录")
        ensure_reline(conn,ticket,focus)
        if rating["category"]!=Finding.NORMAL or focus["ring_index"]>.36:
            raise HTTPException(409,"复线后仍有异常或明显回响，保留隔离")
        conn.execute(points.update().where(points.c.code==point["code"]).values(patch=Patch.LIVE))
        conn.execute(tickets.update().where(tickets.c.code==code).values(state=Ticket.RESTORED,
                     reline_window=focus["code"],test_score=rating["score"],restored_by=user["sub"]))
        record(conn,user,f"reline:{code}:{focus['code']}")
        return one(conn,tickets,code)
