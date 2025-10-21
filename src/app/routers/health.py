import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_session

router = APIRouter(prefix="/health", tags=["health"])

@router.get("/db", status_code=200)
async def db_health(dbsession: AsyncSession = Depends(get_session)):
    try:
        await dbsession.execute(text("select 1"))
        return {"status": "ok"}
    except OperationalError:
        logging.exception("DB healthcheck failed: OperationalError")
        raise HTTPException(status_code=503, detail="database unavailable")
    except SQLAlchemyError:
        logging.exception("DB healthcheck failed: SQLAlchemyError")
        raise HTTPException(status_code=500, detail="database error")