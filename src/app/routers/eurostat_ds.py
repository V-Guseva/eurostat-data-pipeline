from datetime import datetime, date
from typing import Optional, Annotated, Union, List

from fastapi import Query, APIRouter, HTTPException
from fastapi.params import Depends
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_session
from app.models.eurostat_ds import Eurostat_ds

router_eu_ds = APIRouter(prefix="/eu_ds", tags=["eu_ds"])


@router_eu_ds.get("/ds", status_code=200)
async def get_eurostat_ds(code: Annotated[
                            Optional[str],
                            Query(
                                description="Select ds with code contains in code")]=None,
                          updated_after: Annotated[
                            Optional[Union[datetime, date]],
                            Query(
                                description="Select records where updated_at after this date. ISO8601: '2025-10-01' или '2025-10-01T12:00:00Z'")]= None,
                          start_year: Annotated[
                              Optional[List[int]],
                              Query(description="Select all ds with mentioned start_year")] = None,
                          period: Annotated[
                              Optional[str],
                              Query(description="Select all ds with mentioned period {ytd,A,Q}")]=None,
                          order_by: Annotated[Optional[str],Query(description="Order by")]=None,
                          direction: Annotated[Optional[str],Query(description="Direction, if should be in descending order use desc")]="asc",
                          limit: Annotated[
                              int,
                              Query(ge=1, le=500, description="Select mentioned number of rows")]=100,
                          offset: Annotated[
                              Optional[int],
                              Query(description="Select mentioned number of rows")]=0,
                          dbsession: AsyncSession = Depends(get_session) ):
    """

    :param code: the code of the data set
    :param start_year: the start year of the data set, can be a list
    :param updated_after: filters records to get the nearest updated ds
    :param period: query records to get all for only one chosen period
    :param dbsession: injection to connect to the database
    :param limit: amount of records to return
    :param offset: start index of records to return
    :param order_by: order the records according to the order
    :return:
    For example:
    http://0.0.0.0:8000/eu_df/ds?updated_after=2025-10-01
    http://0.0.0.0:8000/eu_df/ds?code=LFST
    http://0.0.0.0:8000/eu_df/ds?code=LFST&updated_after=2025-10-01
    http://0.0.0.0:8000/eu_df/ds?start_year=2021&start_year=2022
    http://0.0.0.0:8000/eu_ds/ds?period=A&order_by=start_year&limit=5&offset=5
    """
    try:
        stmt = select(Eurostat_ds)
        if code is not None:
            stmt = stmt.filter(Eurostat_ds.code.contains(code,autoescape=True))
        if start_year is not None:
            stmt = stmt.filter(Eurostat_ds.start_year.in_(start_year))
        if updated_after is not None:
            stmt = stmt.filter(Eurostat_ds.last_update_timestamp >= updated_after)
        if period is not None:
            stmt = stmt.filter_by(period = period)
        if order_by is not None:
            col = Eurostat_ds.order_column(order_by)
            if not col:
                raise HTTPException(422, f"Unsupported order_by: {order_by}")
            stmt = stmt.order_by(col.desc() if direction == "desc" else col.asc())
        stmt = stmt.limit(limit)
        stmt = stmt.offset(offset)
        result = await dbsession.execute(stmt)
        records = result.mappings().all()
        return records
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router_eu_ds.get("/ds-inspect")
async def inspect_eurostat_ds_db(session: AsyncSession = Depends(get_session)):
    async_engine = session.bind  # AsyncEngine

    def do_reflect(sync_conn):
        insp = sa_inspect(sync_conn)
        schema = "staging"
        table = "eurostat_ds"
        cols_out = []
        for c in insp.get_columns(table, schema=schema):
            cols_out.append({
                "name": c["name"],
                "type": type(c["type"]).__name__,
                "length": getattr(c["type"], "length", None),
                "nullable": bool(c.get("nullable", True)),
                "primary_key": c.get("primary_key", False),
                "default": c.get("default"),
            })
        return cols_out

    async with async_engine.begin() as conn:
        cols = await conn.run_sync(do_reflect)

    return cols


@router_eu_ds.get("/ds-count", status_code=200)
async def get_eurostat_ds_count(group_by: Annotated[
                            Optional[List[str]],
                            Query(
                                description="Group by mentioned fields")
                          ]=None,
                                order_by: Annotated[Optional[str],Query(description="Order by")]=None,
                                direction: Annotated[Optional[str],Query(description="Direction, if should be in descending order use desc")]="asc",
                                dbsession: AsyncSession = Depends(get_session)):
    """
    :param group_by column names to group by
    :param dbsession: injection to connect to the database
    :return:
    For example:
    http://0.0.0.0:8000/eu_ds/ds-count
    http://0.0.0.0:8000/eu_ds/ds-count?group_by=end_year&group_by=start_year
    http://0.0.0.0:8000/eu_ds/ds-count?group_by=end_year&group_by=start_year&order_by=end_year
    """
    try:
        if group_by is None:
            raise HTTPException(status_code=404, detail="No group_by specified")
        columns = []
        for col_name in group_by:
            col = Eurostat_ds.group_column(col_name)
            if col is None:
                raise HTTPException(status_code=404, detail=f"No such column found to group_by: {col_name}")
            columns.append(col)
        stmt = (select(  *columns, func.count().label("count"))
            .select_from(Eurostat_ds)
            .group_by(*columns))
        if order_by is not None:
            col = Eurostat_ds.order_column(order_by)
            if not col:
                raise HTTPException(422, f"Unsupported order_by: {order_by}")
        stmt = stmt.order_by(col.desc() if direction == "desc" else col.asc())
        result = await dbsession.execute(stmt)
        records = result.mappings().all()
        return records
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

