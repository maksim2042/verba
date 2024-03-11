import base64
import copy
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

from tm_import import utils

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

class TrademarkStorage:
    def __init__(self, connection_string, alive_tm_statuses, connect_args={}):
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

    def fetch_data_for_RAG(self):
        statement = """
            SELECT
                tm.*, 
                json_group_array(
                    json_object( 
                        'code', type_code, 
                        'description', description
                    )
                ) statement, 
                f.status, 
                f.alive,
                f.MaxDate
            FROM
                trademark as tm
            LEFT JOIN 
                statement as st
            ON st.trademark_serial = tm.serial_number 
            LEFT JOIN 
                (SELECT  *, MAX(date) MaxDate FROM filing GROUP BY trademark_serial) f
            ON tm.serial_number = f.trademark_serial 
            GROUP BY
                serial_number
        """
        with Session(self._engine) as session:
            for i, res in enumerate(session.execute(sqlalchemy.text(statement))):
                serial, name, owners, statements, status, alive, date = res
                alive = bool(alive)
                if not alive:
                    continue
                owners = json.loads(owners)
                statements = json.loads(statements)
                yield {
                    'serial-number': serial,
                    'trademark-name': name,
                    'owners': owners,
                    'statements': statements,
                    'status': status,
                    'alive': alive,
                    'filing-date': date
                }

    def flush(self):
        self._session.commit()


class TrademarkStorageOLD:
    def __init__(self, storage_file):
        self._data = {}
        self._alive_companies = set()
        self._storage_file = storage_file
        self._buffer_created = []
        self._buffer_deleted = []


    def update(self, tm):
        serial_number = tm['serial-number']
        mark = tm['mark']
        status = tm['status']
        transaction_date = tm['transaction-date']
        statements = tm['statements']
        assert type(serial_number) is str
        assert type(mark) is str
        assert type(status) is int
        assert type(transaction_date) is str and len(transaction_date) == 8

        existed = self._data.get(serial_number)
        if not existed or existed['transaction-date'] < transaction_date:
            self._data[serial_number] = {
                'mark': mark,
                'status': status,
                'transaction-date': transaction_date,
                'statements': statements
            }
            self._alive_companies.update(serial_number)
        else:
            print() #TODO: this was a place for the debugger to stop (strange case)

    def remove(self, tm):
        serial_number = tm['serial-number']
        if serial_number in self._alive_companies:
            self._alive_companies.remove(serial_number)
            self._buffer_deleted.append(tm)

    def drain_the_accumulation(self):
        c = self._buffer_created
        d = self._buffer_deleted
        self._buffer_created = []
        self._buffer_deleted = []
        return c, d

    def dump(self):
        pass
        # with open(self._storage_file, 'wb') as file:
        #     pickle.dump(data, file)
        # with open(self._storage_file, 'w') as file:
        #     json.dump(self._data, file, indent=4)

    def load(self):
        self._data = {}
        # try:
        #     with open(self._storage_file) as file:
        #         self._data = json.load(file)
        # except FileNotFoundError:
        #     logger.warning(
        #         f"File '{self._storage_file}' wasn't found. "
        #         f"This is normal only if the script is run for the first time and the data has not been collected yet"
        #     )
        #     self._data = {}
