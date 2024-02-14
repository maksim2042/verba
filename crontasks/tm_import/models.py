import json
import logging
from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

logger = logging.getLogger(__name__)

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "trademark"

    id: Mapped[int] = mapped_column(primary_key=True)
    serial_number: Mapped[str] = mapped_column(String(30))
    mark: Mapped[str] = mapped_column(String(128))



    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"


class TrademarkStorage:
    def __init__(self, storage_file):
        self._data = {}
        self._storage_file = storage_file

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
        if not existed :
            self._data[serial_number] = {
                'mark': mark,
                'status': status,
                'transaction-date': transaction_date,
                'statements': statements
            }
        elif existed['transaction-date'] < transaction_date:
            self._data[serial_number] = {
                'mark': mark,
                'status': status,
                'transaction-date': transaction_date,
                'statements': statements
            }
        else:
            print() #TODO: this was a place for the debugger to stop (strange case)

    def remove(self, tm):
        serial_number = tm['serial-number']
        if serial_number in self._data:
            del self._data[serial_number]

    def dump(self):
        with open(self._storage_file, 'w') as file:
            json.dump(self._data, file, indent=4)

    def load(self):
        try:
            with open(self._storage_file) as file:
                self._data = json.load(file)
        except FileNotFoundError:
            logger.warning(
                f"File '{self._storage_file}' wasn't found. "
                f"This is normal only if the script is run for the first time and the data has not been collected yet"
            )
            self._data = {}
