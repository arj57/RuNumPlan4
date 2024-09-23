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


OutRecord = dict[str, typing.Any]
# class OutRecord(TypedDict):
#     abc: int
#     begin: int
#     end: int
#     capacity: int
#     # operator: str
#     # region: str
#     reg_gar_id: str
#     inn: int


OutRecords = list[OutRecord]


class OutData(NamedTuple):
    metadata: Metadata
    records: OutRecords


class DbConnectionParameters(TypedDict):
    host: str
    user: str
    password: str
    database: str
    port: int


class LocData(NamedTuple):
    parent_id: Optional[bytes]
    level: int
    title: str


class Location(NamedTuple):
    id: bytes
    data: LocData


class TT(NamedTuple):
    curr_locations: list[Location]
    joined_names: str


IdTitle = dict[int, str]
# class IdTitle(TypedDict):
#     id: int
#     title: str
