import csv
import logging
import os
from collections.abc import Iterator
from datetime import datetime, timezone
from data_types import Metadata
import data_types
from url import Url
from abc import abstractmethod
import config
import utils

logger = logging.getLogger('logger')


class AbstractReader(object):

    def __init__(self, global_config: config.Config, src_url: Url):
        self.config: config.Config = global_config
        self.src_fields: tuple[str, ...] = self.get_src_fields()
        self.url: Url = src_url
        dd = global_config.get_param_val(r'InputDataDir')
        self.abs_tmp_inp_dir = utils.get_abs_path(dd)

        logger.info("Src Url: %s Tmp input dir: %s" % (self.url.string, self.abs_tmp_inp_dir))

    def get_parsed_data(self, skip_header: bool = True) -> data_types.InpData:
        csv_extra_parameters = self.get_csv_extra_parameters()
        csv.register_dialect('custom-dialect', **csv_extra_parameters)

        self.copy_data_to_tmp()
        src_abs_path = os.path.join(self.abs_tmp_inp_dir, os.path.basename(self.url.path))
        with open(src_abs_path, 'r') as fin:
            logger.info("Читаем данные из файла '%s'..." % src_abs_path)
            lines = fin.readlines()
        rec_iter: Iterator = csv.DictReader(lines, fieldnames=self.src_fields, dialect='custom-dialect')
        if skip_header:
            next(rec_iter, None)  # skip header
        metadata: list[data_types.Metadata] = [self._get_metadata(src_abs_path)]

        return data_types.InpData(metadata=metadata, records=list(rec_iter))

    def _get_metadata(self, src_abs_path: str) -> Metadata:
        dt_modified = datetime.fromtimestamp(os.path.getmtime(src_abs_path), tz=timezone.utc)
        file_len: int = os.path.getsize(src_abs_path)
        return Metadata(src_url=self.url.scheme + "://" + src_abs_path, created_date=dt_modified, len=file_len)

    @abstractmethod
    def copy_data_to_tmp(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    def get_src_fields(self) -> tuple[str, ...]:
        res = []
        for node in self.config.findall(r'Converter/SrcFields/Field'):
            res.append(self.config.get_param_val(r'Name', node))

        return tuple(res)

    def get_csv_extra_parameters(self) -> dict[str,str]:
        res: dict[str, str] = {}
        for n in self.config.findall(r"Src/CsvExtraParameters/*"):
            k = n.tag
            v = self.config.get_param_val(r".", n)
            res[k] = v

        return res

