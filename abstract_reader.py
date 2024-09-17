import csv
import logging
import os

import data_types
from url import Url
from abc import abstractmethod
import config

logger = logging.getLogger('logger')


class AbstractReader(object):

    def __init__(self, global_config: config.Config):
        self.config: config.Config = global_config
        self.src_fields: tuple[str, ...] = self.get_src_fields()
        self.url = Url(self.config.get_param_val(r"./Src/Url"))
        self.input_dir = global_config.get_param_val('InputDataDir')

        logger.info("Src Url: %s" % self.url)

    @abstractmethod
    def get_filelist(self) -> list[str]:
        pass

    @abstractmethod
    def download_data(self, filename: str) -> None:
        pass

    def get_parsed_data(self, filename: str) -> datatypes.InpData:
        os.chdir(self.input_dir)
        with open(filename, 'r') as fin:
            logger.info("Читаем данные из файла '%s'..." % filename)
            cdr_list = fin.readlines()
        csv_extra_parameters = self.get_csv_extra_parameters()
        csv.register_dialect('custom-dialect', ** csv_extra_parameters)
        src_data: datatypes.InpRecords = list(csv.DictReader(cdr_list,
                                                             fieldnames=self.src_fields,
                                                             dialect='custom-dialect')
                                              )
        return datatypes.InpData(src_filename=filename, dst_filename=filename, cdr_records=src_data)

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
