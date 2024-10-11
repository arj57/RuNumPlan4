from abstract_reader import AbstractReader
import app_logger
import config
import csv
import data_types
import os.path
import shutil
from collections.abc import Iterator
from datetime import datetime, timezone
from overrides import override
from data_types import Metadata
from url import Url

logger = app_logger.get_logger(__name__)


def get_abs_path(filename: str) -> str:
    if os.path.isabs(filename):
        abs_path = filename
    else:
        wd = os.path.abspath(os.path.curdir)
        src_basename = os.path.basename(filename)
        src_dirname =  os.path.dirname(filename)
        os.chdir(src_dirname)
        abs_path = os.path.abspath(src_basename)
        os.chdir(wd)
    return abs_path


class FileReader(AbstractReader):
    def __init__(self, global_config: config.Config, src_url: Url):
        super().__init__(global_config, src_url)

        if not self.tmp_inp_dir.startswith('/'):
            self.tmp_inp_dir = os.path.join(os.path.dirname(__file__), 'InputData')

    @override()
    def get_parsed_data(self, skip_header: bool=True) -> data_types.InpData:
        # self.copy_data_to_tmp()
        csv_extra_parameters = self.get_csv_extra_parameters()
        csv.register_dialect('custom-dialect', **csv_extra_parameters)

        src_abs_path = get_abs_path(self.url.path)  # self.url.path may be also relative
        with open(src_abs_path, 'r') as fin:
            logger.info("Читаем данные из файла '%s'..." % src_abs_path)
            lines = fin.readlines()
        rec_iter: Iterator = csv.DictReader(lines, fieldnames=self.src_fields, dialect='custom-dialect')
        if skip_header:
            next(rec_iter, None)  # skip header
        metadata: list[data_types.Metadata] = [self._get_metadata(src_abs_path)]

        return data_types.InpData(metadata=metadata, records=list(rec_iter))

    @override()
    def copy_data_to_tmp(self) -> None:
        pass

    def _get_metadata(self, src_abs_path: str) -> Metadata:
        dt_modified = datetime.fromtimestamp(os.path.getmtime(src_abs_path), tz=timezone.utc)
        file_len: int = os.path.getsize(src_abs_path)
        return Metadata(src_url=self.url.scheme + "://" + src_abs_path, created_date=dt_modified, len=file_len)

    @override()
    def close(self) -> None:
        pass