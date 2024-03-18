import dataclasses
import datetime as dt
import json
import logging
from typing import List

import sqlalchemy
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    Session,
)
from sqlalchemy_utils import create_database
from sqlalchemy_utils import database_exists

from crontasks.tm_import import utils

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class Trademark(Base):
    __tablename__ = "trademark"

    serial_number: Mapped[str] = mapped_column(sqlalchemy.String(32), primary_key=True)
    mark: Mapped[str] = mapped_column(sqlalchemy.TEXT)
    owners = mapped_column(sqlalchemy.JSON)
    statements = mapped_column(sqlalchemy.JSON)

    filings: Mapped[List["TrademarkFiling"]] = relationship(
        back_populates="trademark",
        cascade="all, delete-orphan"
    )


class TrademarkFiling(Base):
    __tablename__ = "filing"

    id: Mapped[int] = mapped_column(primary_key=True)
    trademark_serial: Mapped[str] = mapped_column(sqlalchemy.ForeignKey("trademark.serial_number"))
    status: Mapped[int] = mapped_column(sqlalchemy.Integer)
    alive: Mapped[bool] = mapped_column(sqlalchemy.Boolean)
    date: Mapped[dt.date] = mapped_column(sqlalchemy.Date)

    trademark: Mapped["Trademark"] = relationship(back_populates="filings")

    __table_args__ = (
        sqlalchemy.UniqueConstraint('trademark_serial', 'status', 'date'),
    )


@dataclasses.dataclass
class TrademarkData:
    serial_number: str
    name: str
    owners: list[str]
    statements: list[dict]
    status: int
    alive: bool
    filing_date: dt.date

    @property
    def name_in_verba(self):
        return f"TM{self.serial_number} {self.name[:10]}"

    def as_json_string(self):
        def json_serial(obj):
            """JSON serializer for objects not serializable by default json code"""
            if isinstance(obj, (dt.date, dt.datetime)):
                return obj.isoformat()
            raise TypeError("Type %s not serializable" % type(obj))

        return json.dumps(dataclasses.asdict(self), indent=4, default=json_serial)


class TrademarkStorage:
    def __init__(self, connection_string, alive_tm_statuses=None, connect_args={}):
        self._engine = sqlalchemy.create_engine(connection_string, echo=False, connect_args=connect_args)
        if not database_exists(self._engine.url):
            create_database(self._engine.url)
        else:
            self._engine.connect()
        Trademark.__table__.create(bind=self._engine, checkfirst=True)
        TrademarkFiling.__table__.create(bind=self._engine, checkfirst=True)
        self._alive_tm_statuses = alive_tm_statuses
        self._session = Session(self._engine)

    def add_trademark(self, tm, session):
        assert type(self._alive_tm_statuses) is list, "Set 'alive_tm_statuses' parameter to the constructor of this TrademarkStorage"

        serial_number = tm['serial-number']
        mark = tm['mark']
        status = tm['status']
        transaction_date = tm['transaction-date']
        statements = tm['statements']
        owners = tm['owners']
        assert type(serial_number) is str
        assert type(mark) is str
        assert type(status) is int
        assert type(transaction_date) is str and len(transaction_date) == 8


        session.execute(
            sqlalchemy.dialects.postgresql.insert(Trademark).
            values(
                serial_number=serial_number,
                mark=mark,
                owners=list(set([o.get('party-name') for o in owners]) - {None}),
                statements=statements
            ).
            on_conflict_do_nothing()
        )

        session.execute(
            sqlalchemy.dialects.postgresql.insert(TrademarkFiling).
            values(
                trademark_serial=serial_number,
                status=status,
                alive=status in self._alive_tm_statuses,
                date=utils.parse_date_string(transaction_date),
            ).
            on_conflict_do_nothing()
        )

    def flush(self):
        self._session.commit()

    def fetch_trademarks_data_from_db(self, dates=None):
        if not dates:
            statement = utils.load_sql_template('fetch_actual_trademarks.sql')

        with Session(self._engine) as session:
            for i, res in enumerate(session.execute(sqlalchemy.text(statement))):
                serial, name, owners, statements, status, alive, date = res

                yield TrademarkData(
                    serial_number=serial,
                    name=name,
                    owners=owners,
                    statements=statements,
                    status=status,
                    alive=alive,
                    filing_date=date,
                )
