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
        self.op_id_titles: data_types.IdTitles = {}
        self.loc_objects: data_types.Locations = {}
        self.total_loc_indexes: list[bytes] = []

    def get_converted_data(self, input_data: data_types.InpData) -> data_types.OutData:
        logger.info("Конвертируем...")
        src_records: data_types.InpRecords = input_data.records
        out_rows: data_types.OutRows = []

        for i, src_row in enumerate(src_records, start=1):
            try:
                out_row_and_ref: data_types.OutRowAndRef = self.convert_row(src_row)
                out_rows.append(out_row_and_ref.row)
                self.store_op_id_title(out_row_and_ref.op_id_title.id, out_row_and_ref.op_id_title.title)
                self.store_locations(out_row_and_ref.loc_objects)
                if i % 10000 == 0:
                    print('.', sep='', end='', flush=True)
            except ValueError or utils.OptionalFieldEmptyException as msg:
                print('')
                logger.debug("Игнорируем строку %s:  %s.", src_row, msg)
                continue
            except KeyError as msg:
                if self.config.get_param_val(r"Converter/OnKeyErrorAction") == 'WARNING':
                    print('')
                    logger.warning('В строке %s: %s' % (src_row, msg))
                    continue
                else:
                    print('')
                    logger.error('В строке %s: %s' % (src_row, msg))
                exit(1)

        print('')
        res = data_types.OutData(metadata=input_data.metadata, rows=out_rows, op_id_titles=self.op_id_titles,
                                 loc_objects=self.loc_objects)
        return res

    def store_op_id_title(self, op_id: int, title: str) -> None:
        self.op_id_titles.setdefault(op_id, title)

    def store_locations(self, curr_locations: list[data_types.Location]) -> None:
        for loc in curr_locations:
            self.loc_objects.setdefault(loc.id, loc)

    @abstractmethod
    def convert_row(self, src_row: data_types.InpRecord) -> data_types.OutRowAndRef:
        pass

    def get_dst_fields_metadata(self) -> dict[str, DstFieldMetadata]:
        res: dict[str, DstFieldMetadata] = {}
        for dst_field_node in self.config.findall(r"Converter/DstFields/"):
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

