#! /usr/bin/python
# -*- coding: utf-8 -*-
import typing
from abc import abstractmethod
from datetime import datetime

import app_logger
import utils
from config import Config
import data_types

logger = app_logger.get_logger(__name__)


class DstFieldMetadata(typing.NamedTuple):
    dst_field_type: str
    src_field_name:  str
    action_name: typing.Optional[str]


class AbstractConverter(object):

    def __init__(self, config: Config) -> None:
        self.config = config
        self.dst_fields_metadata_map: dict[str, DstFieldMetadata] = self.get_dst_fields_metadata()
        self.op_id_titles: data_types.IdTitle = {}
        self.locations: data_types.Locations = []
        self.total_loc_indexes: list[bytes] = []

    def get_converted_data(self, input_data: data_types.InpData) -> data_types.OutData:
        """
        Формирует данные в виде словаря:
        'src_filename': str - имя исходного файла из SMG
        'dst_filename': str - имя результирующего файла
        'data': datatypes.Records - список CDR, сконвертированный
        """
        logger.info("Конвертируем...")
        src_records: data_types.InpRecords = input_data.records
        out_records: data_types.OutRecords = []

        for i, src_row in enumerate(src_records, start=1):
            try:
                out_record: data_types.OutRecord = self.convert_row(src_row)
                out_records.append(out_record)
            except utils.OptionalFieldEmptyException as msg:
                logger.debug("Игнорируем строку: %d в файле \"%s\":  %s.",
                             i, input_data.metadata.src_filename, msg)
                continue
            except ValueError as msg:
                logger.debug("Игнорируем строку : %d в файле \"%s\":  %s.",
                               i, input_data.metadata.src_filename, msg)
                continue
            except KeyError as msg:
                if self.config.get_param_val(r"/Converter/OnKeyErrorAction") == 'WARNING':
                    logger.warning('В строке %d в файле "%s": %s' % (i, input_data.metadata.src_filename, msg))
                    continue
                else:
                    logger.error('В строке %d в файле "%s": %s' % (i, input_data.metadata.src_filename, msg))
                exit(1)

        res = data_types.OutData(metadata=input_data.metadata, records= out_records)
        return res

    @abstractmethod
    def convert_row(self, src_row: data_types.InpRecord) -> data_types.OutRecord:
        pass

    def get_dst_fields_metadata(self) -> dict[str, DstFieldMetadata]:
        res: dict[str, DstFieldMetadata] = {}
        for dst_field_node in self.config.findall(r"/Converter/DstFields/"):
            dst_field_name = self.config.find(r'Name', dst_field_node).text
            src_field_name = self.config.find(r'Source', dst_field_node).text
            action_name: typing.Optional[str] = self.get_action_name(dst_field_node)
            dst_field_type: str = self.get_field_type(dst_field_node)
            res[dst_field_name] = DstFieldMetadata(src_field_name=src_field_name, action_name=action_name, dst_field_type=dst_field_type)

        return res

    def get_action_name(self, node) -> typing.Optional[str]:
        action_node = self.config.find(r'Action', node)
        if action_node is not None:
            return action_node.text
        else:
            return None

    def get_field_type(self, node) -> str:
        type_node = self.config.find(r'Type', node)
        if type_node is not None:
            return type_node.text
        else:
            return 'str'

    def typecast_field(self, src_val: str, dst_field_name: str) -> typing.Any:
        t = self.dst_fields_metadata_map[dst_field_name].dst_field_type
        if src_val is None:
            return src_val
        elif t == 'int':
            return int(src_val)
        elif t == 'datetime':
            return datetime.strptime(src_val, '%Y-%m-%d %H:%M:%S')
        else:
            return src_val

