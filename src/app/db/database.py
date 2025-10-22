from __future__ import annotations

import asyncio
import logging

from fastapi import Depends
from sqlalchemy import inspect, text, MetaData, Column, Table, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

DATABASE_URL = "postgresql+psycopg://staging_ro:dsf78%$fd@db:5432/demo"

engine = create_async_engine(
    DATABASE_URL,
    echo=True,
    pool_pre_ping=True, # check connection
    connect_args={"connect_timeout": 5},  # optional but helpful for health
)

SessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False, future=True) # async_sessionmaker - factory to create sessions

# This dependency takes NO args -> FastAPI won’t require query params
async def get_session() -> AsyncSession:
    async with SessionLocal() as session:
        yield session


async def list_tables(schema: str = "staging"):
    async with get_session() as session:               # AsyncSession
        async with session.connection() as conn:       # -> AsyncConnection
            return await conn.run_sync(
                lambda sc: inspect(sc).get_table_names(schema=schema)
            )

async def list_tables_sql(schema: str = "staging"):
    async with engine.connect() as conn:
        r = await conn.execute(
            text("""
                select current_schema()
            """),
            {"s": schema},
        )
        return [row[0] for row in r]

async def get_data(dbsession: AsyncSession = Depends(get_session)):
    r = await dbsession.execute(
            text("""
                select *
                from staging.eurostat_ds
                where code = 'LFST_HHTEMCHI'
            """),
        )
    logging.error(f"we are here {r}")
    for a in r:
        logging.info(a)
    return [row[0] for row in r]

@staticmethod
def enable_db_info_log():
    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.dialects").setLevel(logging.INFO)