import typing
from abc import abstractmethod

import app_logger
import data_types
from datetime import datetime

logger = app_logger.get_logger(__name__)


class AbstractMetadata:
    # Работает с уже сконвертированными данными
    def __init__(self, data: data_types.InpData):
        self.CDR_DATETIME_FORMAT: str = "%Y-%m-%d %H:%M:%S"
        self.nas_id: int = 410
        self.data: data_types.InpData = data
        self.from_file: str = data['src_filename']
        self.date_from: datetime = self.get_records_date_from(self.data['cdr_records'])
        self.date_to: datetime = self.get_records_date_to(self.data['cdr_records'])

    def get_records_date_from(self, cdr_records: data_types.InpRecords) -> typing.Optional[datetime]:
        dt: typing.Optional[datetime] = None
        for record in cdr_records:
            if record["redir"] in [0, 1]:
                dt = datetime.strptime(record["t_release"], self.CDR_DATETIME_FORMAT)
                break
        return dt

    def get_records_date_to(self, cdr_records: data_types.InpRecords) -> typing.Optional[datetime]:
        dt = None
        for record in reversed(cdr_records):
            if record["redir"] in [0, 1]:
                dt = datetime.strptime(record["t_release"], self.CDR_DATETIME_FORMAT)
                break
        return dt

    def get_metadata(self) -> data_types.TMetadata:
        return {'loaded_file': self.from_file,
                'nas_id': self.nas_id,
                'records': len(self.data['cdr_records']),
                'dt_start': self.date_from,
                'dt_end': self.date_to,
                'start_id': None,
                'end_id': None
                }

    @abstractmethod
    def save(self) -> None:
        raise NotImplementedError
