#! /usr/bin/python
# -*- coding: utf-8 -*-

from abc import abstractmethod

import app_logger
import utils
from config import Config
import data_types

logger = app_logger.get_logger(__name__)


class AbstractConverter(object):

    def __init__(self, config: Config) -> None:
        self.config = config

    def get_converted_data(self, cdr_data: datatypes.InpData) -> datatypes.OutData:
        """
        Формирует данные в виде словаря:
        'src_filename': str - имя исходного файла из SMG
        'dst_filename': str - имя результирующего файла
        'data': datatypes.Records - список CDR, сконвертированный
        """
        logger.info("Конвертируем...")

        src_filename: str = cdr_data.src_filename
        src_records: datatypes.InpRecords = cdr_data.cdr_records

        out_records: datatypes.OutRecords = []
        for i, src_row in enumerate(src_records, start=1):
            try:
                dst_record: datatypes.OutRecord = self.convert_row(src_row)
                out_records.append(dst_record)
            except utils.OptionalFieldEmptyException as msg:
                logger.debug("Игнорируем строку: %d в файле \"%s\":  %s.",
                             i, src_filename, msg)
                continue
            except ValueError as msg:
                # logger.debug("Игнорируем строку: %d в файле \"%s\":  %s.",
                #                i, src_filename, msg)
                continue
            except KeyError as msg:
                if self.config.get_param_val(r"/Converter/OnKeyErrorAction") == 'WARNING':
                    logger.warning('В строке %d в файле "%s": %s' % (i, src_filename, msg))
                    continue
                else:
                    logger.error('В строке %d в файле "%s": %s' % (i, src_filename, msg))
                exit(1)

        res = datatypes.OutData(src_filename= src_filename, dst_filename=src_filename, cdr_records= out_records)

        return res

    @abstractmethod
    def convert_row(self, src_row: datatypes.InpRecord) -> datatypes.OutRecord:
        pass
