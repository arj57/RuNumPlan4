# import typing
import typing
from datetime import datetime
from typing import TypedDict, NamedTuple, Optional


class Metadata(NamedTuple):
    src_filename: str
    created_date: datetime

# Only type = str for csv
InpRecord = dict[str, str]
InpRecords = list[InpRecord]


class InpData(NamedTuple):
    metadata: Metadata
    records: InpRecords


OutRow = dict[str, typing.Any]
# class OutRecord(TypedDict):
#     abc: int
#     begin: int
#     end: int
#     capacity: int
#     # operator: str
#     # region: str
#     reg_gar_id: str
#     inn: int


OutRows = list[OutRow]


class OutData(NamedTuple):
    metadata: Metadata
    records: OutRows


class DbConnectionParameters(TypedDict):
    host: str
    user: str
    password: str
    database: str
    port: int




class Location(NamedTuple):
    class Value(NamedTuple):
        parent_id: Optional[bytes]
        level: int
        title: str

    id: bytes
    data: Value



IdTitle = dict[int, str]
# class IdTitle(TypedDict):
#     id: int
#     title: str
