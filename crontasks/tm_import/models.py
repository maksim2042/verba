import datetime as dt
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
    mark: Mapped[str] = mapped_column(sqlalchemy.String(128))
    owners = mapped_column(sqlalchemy.JSON)

    statements: Mapped[List["TrademarkStatement"]] = relationship(
        back_populates="trademark",
        cascade = "all, delete-orphan"
    )
    filings: Mapped[List["TrademarkFiling"]] = relationship(
        back_populates="trademark",
        cascade="all, delete-orphan"
    )

    # def __repr__(self) -> str:
    #     return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"


class TrademarkStatement(Base):
    __tablename__ = "statement"

    id: Mapped[int] = mapped_column(primary_key=True)
    trademark_serial: Mapped[str] = mapped_column(sqlalchemy.ForeignKey("trademark.serial_number"))
    type_code: Mapped[str] = mapped_column(sqlalchemy.String(32))
    description: Mapped[str] = mapped_column(sqlalchemy.Text)

    trademark: Mapped["Trademark"] = relationship(back_populates="statements")


class TrademarkFiling(Base):
    __tablename__ = "filing"

    id: Mapped[int] = mapped_column(primary_key=True)
    trademark_serial: Mapped[str] = mapped_column(sqlalchemy.ForeignKey("trademark.serial_number"))
    status: Mapped[int] = mapped_column(sqlalchemy.Integer)
    alive: Mapped[bool] = mapped_column(sqlalchemy.Boolean)
    date: Mapped[dt.date] = mapped_column(sqlalchemy.Date)

    trademark: Mapped["Trademark"] = relationship(back_populates="filings")


class TrademarkStorage:
    def __init__(self, connection_string, alive_tm_statuses):
        self._engine = sqlalchemy.create_engine(connection_string, echo=False)
        if not database_exists(self._engine.url):
            create_database(self._engine.url)
        else:
            self._engine.connect()
        Trademark.__table__.create(bind=self._engine, checkfirst=True)
        TrademarkStatement.__table__.create(bind=self._engine, checkfirst=True)
        TrademarkFiling.__table__.create(bind=self._engine, checkfirst=True)
        self._alive_tm_statuses = alive_tm_statuses

    def add_trademark(self, tm):
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

        with Session(self._engine) as session:
            tm = session.query(Trademark).where(Trademark.serial_number == serial_number).first()
            if tm is None:
                tm = Trademark(
                    serial_number=serial_number,
                    mark=mark,
                    owners=list(set([o['party-name'] for o in owners]))
                )
                session.add(tm)

            for code, description in statements.items():
                if all([tc.type_code != code for tc in tm.typecodes]):
                    tm.typecodes.append(
                        TrademarkStatement(
                            type_code=code,
                            description=description,
                        )
                    )

            filing_date = utils.parse_date_string(transaction_date)
            if all([filing.date != filing_date for filing in tm.filings]):
                tm.filings.append(
                    TrademarkFiling(
                        status=status,
                        alive=status in self._alive_tm_statuses,
                        date=filing_date
                    )
                )

            session.commit()


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
