import asyncio

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_session

import logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/health", tags=["health"])

@router.get("/db", status_code=200, summary="Health check db connection",
            responses={200: {"description": "DB reached successfully"},
                       500: {"description": "DB driver error"},
                       503: {"description": "DB connection failed"}})
async def db_health(dbsession: AsyncSession = Depends(get_session)):
    try:
        await asyncio.wait_for(dbsession.execute(text("select 1")), timeout=2.0)
        return {"status": "ok"}
    except asyncio.TimeoutError:
        logger.exception("DB healthcheck failed: TimeoutError")
        raise HTTPException(status_code=503, detail="database timeout")
    except OperationalError:
        logger.exception("DB healthcheck failed: OperationalError")
        raise HTTPException(status_code=503, detail="database unavailable")
    except SQLAlchemyError:
        logger.exception("DB healthcheck failed: SQLAlchemyError")
        raise HTTPException(status_code=500, detail="database error")