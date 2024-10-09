import hashlib
import typing
from typing import Callable, Optional, NamedTuple

import data_types
from abstract_converter import AbstractConverter
from config import Config
from data_types import Location, OutRowAndRef


def _my_hash(_string: bytes) -> bytes:
    return hashlib.md5(_string, usedforsecurity=False).digest()

class TT(NamedTuple):
    curr_locations: list[Location]
    joined_names: str


def _get_location_data(full_location: str) -> list[data_types.Location]:
    """
      Получение списка объектов LocValue из строки с локейшеном ([[населенный пункт|]район|]область)
     :parameter full_location - строка с локейшеном
     :return - список объектов Location ( [{id, parent_id, title}, ...] - для каждого распарсенного локейшена)
    """

    def _get_parsed_locations_list(parsed_locations: list[str],
                                   tmp_data: TT = None) -> TT:
        """
           Получение списка объектов LocValue из списка строк с иерархией локейшенов (область, район, населенный пункт). Вспомогательный метод.
         :parameter parsed_locations - список строк с иерархией локейшенов
         :return - кортеж: список объектов Location ([{id, parent_id, title}, ...] - для каждого распарсенного локейшена), а также joined_names - необходим для рекурс. вызова
         :exception ValueError - if parsed locations list is empty
        """
        if len(parsed_locations) == 0:
            raise ValueError('Parsed locations list is empty')

        partial_name: str = parsed_locations.pop().strip()
        if tmp_data is None:
            locations: list[data_types.Location] = []
            level = 0
            parent_id = None
            joined_names = partial_name
        else:
            locations = tmp_data.curr_locations
            level: int = len(locations)
            parent_id: Optional[bytes] = locations[-1].id
            joined_names = tmp_data.joined_names + '|' + partial_name

        loc_key: bytes = _my_hash(joined_names.encode('utf-8'))
        location = data_types.Location(id=loc_key, parent_id=parent_id, level=level, title=partial_name)
        locations.append(location)
        tmp_data = TT(curr_locations=locations, joined_names=joined_names)

        if len(parsed_locations) == 0:
            return tmp_data
        else:
            return _get_parsed_locations_list(parsed_locations, tmp_data)

    loc_parts: list[str] = full_location.strip().rsplit(sep='|', maxsplit=2)  # [point], [rayon], oblast
    return _get_parsed_locations_list(loc_parts).curr_locations


class NumPlanConverter(AbstractConverter):
    def __init__(self, config: Config) -> None:
        super().__init__(config)
        # self.actions: dict[str, Callable[[str], bool]] = {}

    def convert_row(self, src_row: data_types.InpRecord) -> data_types.OutRowAndRef:
        # {'ABC': '800', 'CAPACITY': '10000', 'FROM': '1000000', 'GAR_REGION': 'Российская Федерация', 'INN': '7707049388', 'OPERATOR': 'ПАО "Ростелеком"', 'REGION': 'Российская Федерация', 'TO': '1009999'}
        tmp_row: dict[str, typing.Any] = {}
        op_id_title = None
        curr_loc_objects: list[data_types.Location] = []

        for dst_field_name in self.dst_fields_metadata_map.keys():
            src_field_name: str = self.dst_fields_metadata_map[dst_field_name].src_field_name
            src_field_val: str = src_row[src_field_name]

            if dst_field_name == 'op_inn':
                tmp_row[dst_field_name] = int(src_field_val)
                op_id_title = data_types.IdTitle(id=int(tmp_row[dst_field_name]), title=src_row['Operator'])
                # TODO: store op_id_title and location objects in AbstractConverter
                # self.store_operator_id_title(int(tmp_row[dst_field_name]), src_row['Operator'])
            elif dst_field_name == 'loc_id':
                curr_loc_objects: list[data_types.Location] = _get_location_data(src_field_val)
                tmp_row[dst_field_name] = curr_loc_objects[-1].id
                # self.store_locations(curr_loc_objects)
            else:
                tmp_row[dst_field_name] = self.typecast_field(src_field_val, dst_field_name)

        out_row = OutRowAndRef(row=tmp_row, op_id_title=op_id_title, loc_objects=curr_loc_objects)
        return out_row

    # def store_operator_id_title(self, op_id: int, title: str) -> None:
    #     self.op_id_titles.setdefault(op_id, title)

    # def store_locations(self, curr_locations: list[data_types.Location]) -> None:
    #     for loc in curr_locations:
    #         self.loc_objects.setdefault(loc.id, loc)

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


class OptionalFieldEmptyException(Exception):
    pass
