import datetime
from typing import Optional, ClassVar

from sqlalchemy import Integer, DateTime, Text, MetaData, CheckConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped
from sqlalchemy.testing.schema import mapped_column


class Base(DeclarativeBase):
    pass


class Eurostat_ds(Base):
    """
    ORM declarative style description for eurostat_ds
    """
    __tablename__ = "eurostat_ds"
    # TODO add period constraint in db and may be start and end year constraint
    __table_args__ = (
        # constraints FIRST ...
        CheckConstraint(
            "period IN ('Y','Q','A','YTD')",
            name="period_allowed"
        ),
        # ... then table kwargs LAST
        {"schema": "staging"},
    )
    code : Mapped[str] = mapped_column(Text, primary_key=True)
    title :Mapped[Optional[str]] = mapped_column(Text)
    last_update_timestamp :Mapped[datetime.datetime] = mapped_column(DateTime, primary_key=False)
    last_structure_change_timestamp :Mapped[datetime.datetime] = mapped_column(DateTime, primary_key=False)
    start_year :Mapped[Optional[int]] = mapped_column(Integer, primary_key=False)
    end_year :Mapped[Optional[int]] = mapped_column(Integer, primary_key=False)
    period : Mapped[Optional[str]] = mapped_column(Text, primary_key=False)
    load_date : Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, primary_key=False)
    ORDERABLE: ClassVar[dict[str, any]] = {
        "code": code,
        "title": title,
        "last_update_timestamp": last_update_timestamp,
        "start_year": start_year,
        "end_year": end_year,
        "period": period,
        "load_date": load_date,
    }
    GROUPABLE: ClassVar[dict[str, any]] = {
        "start_year": start_year,
        "end_year": end_year,
        "period": period,
        "load_date": load_date
    }

    PUBLIC_FIELDS: ClassVar[set[str]] = {
        "code", "start_year", "period", "last_update_timestamp"
    }

    PERIOD_ALLOWED: ClassVar[set[str]] = {"Y", "Q", "A", "YTD"}

    @classmethod
    def get_column(cls, name: str):
        return getattr(cls, name, None) if name in cls.PUBLIC_FIELDS else None

    @classmethod
    def order_column(cls, name: str):
        return cls.ORDERABLE.get(name)

    @classmethod
    def group_column(cls, name: str):
        return cls.GROUPABLE.get(name)