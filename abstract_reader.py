import csv
import logging
import os
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Optional

import data_types
from data_types import Metadata
from url import Url
from abc import abstractmethod
import config

logger = logging.getLogger('logger')


class AbstractReader(object):

    def __init__(self, global_config: config.Config):
        self.config: config.Config = global_config
        self.src_fields: tuple[str, ...] = self.get_src_fields()
        self.url = Url(self.config.get_param_val(r'./Src/Url'))
        self.tmp_inp_dir = global_config.get_param_val(r'InputDataDir')

        logger.info("Src Url: %s" % self.url)

    @abstractmethod
    def get_filelist(self) -> list[str]:
        pass

    @abstractmethod
    def copy_data_to_tmp(self, filename: str) -> None:
        pass

    @abstractmethod
    def get_metadata(self, filename: str) -> Metadata:
        pass

    def get_parsed_data(self, filename: str, skip_header: bool=False) -> data_types.InpData:
        self.copy_data_to_tmp(filename)
        os.chdir(self.tmp_inp_dir)
        with open(filename, 'r') as fin:
            logger.info("Читаем данные из файла '%s'..." % filename)
            cdr_list = fin.readlines()
        csv_extra_parameters = self.get_csv_extra_parameters()
        csv.register_dialect('custom-dialect', ** csv_extra_parameters)
        inp: Iterator = csv.DictReader(cdr_list, fieldnames=self.src_fields, dialect='custom-dialect')
        if skip_header:
            next(inp, None)  # skip header
        records: data_types.InpRecords = list(inp)
        file_metadata: Metadata = self.get_metadata(filename)
        return data_types.InpData(metadata=file_metadata, records=records)

    @abstractmethod
    def close(self) -> None:
        pass

    def get_src_fields(self) -> tuple[str, ...]:
        res = []
        for node in self.config.findall(r'./Converter/SrcFields/Field'):
            res.append(self.config.get_param_val(r'Name', node))

        return tuple(res)

    def get_csv_extra_parameters(self) -> dict[str,str]:
        res: dict[str, str] = {}
        for n in self.config.findall(r"/Src/CsvExtraParameters/*"):
            k = n.tag
            v = self.config.get_param_val(r".", n)
            res[k] = v

        return res
