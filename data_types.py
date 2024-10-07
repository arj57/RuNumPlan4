# import typing
import typing
from datetime import datetime
from typing import TypedDict, NamedTuple, Optional


class IdTitle(NamedTuple):
    id: int
    title: str


IdTitles = dict[int, str]


class Metadata(NamedTuple):
    src_filename: str
    created_date: datetime


# Only type = str for csv
InpRecord = dict[str, str]
InpRecords = list[InpRecord]


class InpData(NamedTuple):
    metadata: Metadata
    records: InpRecords


class DbConnectionParameters(TypedDict):
    host: str
    user: str
    password: str
    database: str
    port: int


class Location(NamedTuple):
    id: bytes
    parent_id: Optional[bytes]
    level: int
    title: str

Locations = dict[bytes, Location]

OutRow = dict[str, typing.Any]
OutRows = list[OutRow]


class OutRowAndRef(NamedTuple):
    row: OutRow
    op_id_title: IdTitle
    loc_objects: list[Location]


class OutData(NamedTuple):
    metadata: Metadata
    rows: OutRows
    op_id_titles: IdTitles
    loc_objects: dict[bytes, Location]
