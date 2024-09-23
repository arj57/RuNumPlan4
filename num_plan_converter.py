import hashlib
import typing
from typing import Callable, Optional, NamedTuple, TypedDict, Literal

from setuptools.extern import names

import data_types
from abstract_converter import AbstractConverter
from config import Config
from data_types import Location


def _my_hash(_string: bytes) -> bytes:
    return hashlib.md5(_string).digest()


class NumPlanConverter(AbstractConverter):
    def __init__(self, config: Config) -> None:
        super().__init__(config)
        # action должна возвращать False, если после action не нужно заполнять поле в dst_row
        self.actions: dict[str, Callable[[str], bool]] = {}

    def convert_row(self, src_row: data_types.InpRecord) -> data_types.OutRecord:
        # {'ABC': '800', 'CAPACITY': '10000', 'FROM': '1000000', 'GAR_REGION': 'Российская Федерация', 'INN': '7707049388', 'OPERATOR': 'ПАО "Ростелеком"', 'REGION': 'Российская Федерация', 'TO': '1009999'}
        dst_row: data_types.OutRecord = {}
        for dst_field_name in self.dst_fields_metadata_map.keys():
            src_field_name: str = self.dst_fields_metadata_map[dst_field_name].src_field_name
            src_field_val: str = src_row[src_field_name]

            if dst_field_name == 'Inn':
                dst_row[dst_field_name] = int(src_field_val)
                self.store_operator_id_title(int(dst_row[dst_field_name]), src_row['Operator'])
            elif dst_field_name == 'RegionId':
                curr_locations: list[data_types.Location] = self._get_location_data(src_field_val).curr_locations
                dst_row[dst_field_name] = curr_locations[-1].id
                self.store_locations(curr_locations)
            else:
                dst_row[dst_field_name] = self.typecast_field(src_field_val, dst_field_name)

        return dst_row

    def _get_location_data(self, full_location: str) -> Optional[data_types.TT]:
        """
          Получение списка объектов Location из строки с локейшеном ([[населенный пункт|]район|]область)
         :parameter parsed_locations - список строк с иерархией локейшенов
         :return - список объектов Location ( [{id, parent_id, title}, ...] - для каждого распарсенного локейшена)
        """
        loc_parts: list[str] = full_location.strip().rsplit(sep='|', maxsplit=2)  # [point], [rayon], oblast
        return self._get_parsed_locations_list(loc_parts)

    # class LocData(NamedTuple):
    #     parent_id: Optional[bytes]
    #     level: int
    #     title: str
    #
    # class Location(TypedDict):
    #     id: bytes
    #     data: LocData

    # class TT(NamedTuple):
    #     curr_locations: list[data_types.Location]
    #     joined_names: str
    def _get_parsed_locations_list(self, parsed_locations: list[str], tmp_data: data_types.TT = None) -> Optional[data_types.TT]:
        """
           Получение списка объектов Location из списка строк с иерархией локейшенов (область, район, населенный пункт). Вспомогательный метод.
         :parameter parsed_locations - список строк с иерархией локейшенов
         :return - список объектов Location ( [{id, parent_id, title}, ...] - для каждого распарсенного локейшена), а также joined_names - необходим для рекурс. вызова
        """
        if len(parsed_locations) == 0:
            return None
        else:
            partial_name: str = parsed_locations.pop().strip()
            if tmp_data is None:
                current_locations: list[data_types.Location] = []
                level = 0
                parent_id = None
                joined_names = partial_name
            else:
                current_locations = tmp_data.curr_locations
                level: int = len(current_locations)
                parent_id: Optional[bytes] = current_locations[-1].id
                joined_names = tmp_data.joined_names + '|' + partial_name

            loc_key: bytes = _my_hash(joined_names.encode('utf-8'))
            loc_val = data_types.LocData(parent_id=parent_id, level=level, title=partial_name)
            location = Location( id=loc_key, data=loc_val)
            current_locations.append(location)
            tmp_data = data_types.TT(curr_locations=current_locations, joined_names=joined_names)

            if len(parsed_locations) == 0:
                return tmp_data
            else:
                return self._get_parsed_locations_list(parsed_locations, tmp_data)

    def store_operator_id_title(self, id: int, title: str) -> None:
        self.op_id_titles.setdefault(id, title)

    def store_locations(self, curr_locations: list[data_types.Location]) -> None:
        for loc in curr_locations:
            self.locations.setdefault(loc.id, loc.data)

    # """
    # action для поля выполняет некоторые дополнительные действия, не связанные с заполнением dst_val.
    # Например, на основе src_val заполняет какие-то дополнительные структуры данных.
    # Если action возвращает False, то значение в dst_val не заносится
    #
    # В dst_row сохраняется только ИНН оператора. Соответствие ИНН оператора и его названия сохраняется
    #  в структуре self.op_id_titles: data_types.IdTitle
    #
    # В dst_row сохраняется двоичный ключ местоположения оператора.
    #  Соответствие ключа и текстового значения местоположения сохраняется
    #  в структуре self.locations: data_types.Locations
    # """

    # def do_action(self, action_name: Optional[str], src_val: str) -> Optional[bool]:
    #     if action_name is None:
    #         return None
    #
    #     if action_name not in self.actions:
    #         return None
    #
    #     action_func: Optional[Callable[[str], bool]] = self.actions[action_name]
    #     if action_func is not None:
    #         try:
    #             return action_func(src_val)
    #         except KeyError:
    #             if len(src_val) == 0:
    #                 raise OptionalFieldEmptyException('Пустое значение поля ')
    #             else:
    #                 raise KeyError('Не могу выполнить action: Неизвестное значение "%s". ' % src_val)
    #     else:
    #         return True


# if mapper_name is not None:
#     mapper_function: Callable[[str], typing.Any] = self.mappers[mapper_name]
#     if mapper_function is not None:
#         try:
#             res[dst_field_name] = mapper_function(src_val)
#         except KeyError:
#             if len(src_val) == 0:
#                 res[dst_field_name] = ""
#                 # raise OptionalFieldEmptyException('Пустое значение "%s" поля "%s".' %
#                 #                                   (src_val, src_field_name))
#             else:
#                 raise KeyError('Неизвестное значение "%s" поля "%s". Не могу преобразовать' %
#                                (src_val, src_field_name))
#
# else:
#     res[dst_field_name] = src_val
#
# return res

class OptionalFieldEmptyException(Exception):
    pass
