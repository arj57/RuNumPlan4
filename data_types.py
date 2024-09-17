import typing
from datetime import datetime
from typing import TypedDict, NamedTuple


class InpRecord(NamedTuple):
    abc: int
    begin: int
    end: int
    capacity: int
    operator: str
    region: str
    reg_gar: str
    inn: int

InpRecords = list[InpRecord]


class InpData(NamedTuple):
    src_filename: str
    dst_filename: str
    records: InpRecords


class OutRecord(NamedTuple):
    abc: int
    begin: int
    end: int
    capacity: int
    operator: str
    # region: str
    reg_gar: str
    inn: int


OutRecords = list[OutRecord]


class OutData(NamedTuple):
    src_filename: str
    dst_filename: str
    records: OutRecords




class TMetadata(TypedDict):
    loaded_file: str
    nas_id: int
    records: int
    dt_start: datetime
    dt_end: datetime
    start_id: typing.Optional[int]
    end_id: typing.Optional[int]


class DbConnectionParameters(TypedDict):
    host: str
    user: str
    password: str
    database: str
    port: int
