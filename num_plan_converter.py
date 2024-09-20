import hashlib
import typing
from typing import Callable, Optional
import data_types
from abstract_converter import AbstractConverter
from config import Config
from data_types import Location


class NumPlanConverter(AbstractConverter):
    def __init__(self, config: Config) -> None:
        super().__init__(config)
        # action должна возвращать False, если после action не нужно заполнять поле в dst_row
        self.actions: dict[str, Callable[[str], bool]] = {}

    """
    action для поля выполняет некоторые дополнительные действия, не связанные с заполнением dst_val.
    Например, на основе src_val заполняет какие-то дополнительные структуры данных.
    Если action возвращает False, то значение в dst_val не заносится
    
    В dst_row сохраняется только ИНН оператора. Соответствие ИНН оператора и его названия сохраняется
     в структуре self.op_id_titles: data_types.IdTitle
     
    В dst_row сохраняется двоичный ключ местоположения оператора. 
     Соответствие ключа и текстового значения местоположения сохраняется 
     в структуре self.locations: data_types.Locations 
    """
    def convert_row(self, src_row: data_types.InpRecord) -> data_types.OutRecord:
        # {'ABC': '800', 'CAPACITY': '10000', 'FROM': '1000000', 'GAR_REGION': 'Российская Федерация', 'INN': '7707049388', 'OPERATOR': 'ПАО "Ростелеком"', 'REGION': 'Российская Федерация', 'TO': '1009999'}
        dst_row: data_types.OutRecord = {}
        for dst_field_name in self.dst_fields_metadata_map.keys():
            src_field_name: str = self.dst_fields_metadata_map[dst_field_name].src_field_name
            src_val: str = src_row[src_field_name]
            dst_row[dst_field_name] = self.typecast_field(src_val, dst_field_name)

            if dst_field_name == 'INN':
                self.store_operator_id_title(dst_row[dst_field_name], src_row['OPERATOR'])

            if dst_field_name == 'REGION':
                self.store_location_data(dst_row[dst_field_name])
                pass
            # action_name: Optional[str] = self.dst_fields_metadata_map[dst_field_name].action_name
            # try:
            #     if self.do_action(action_name, src_val) is None or self.do_action(action_name, src_val) == True :
            #         dst_row[dst_field_name] = self.typecast_field(src_val, dst_field_name)
            # except KeyError as e:
            #     raise KeyError(str(e) + ' поля "%s"' % src_field_name)
            # except OptionalFieldEmptyException as e:
            #     raise OptionalFieldEmptyException(str(e) + ' поля "%s"' % src_field_name)
        return dst_row

    def store_location_data(self, full_location: str):
        # REGION:       г. Опочка|р-н Опочецкий|Псковская обл.
        # GAR_REGION:   г. Опочка|г.п. Опочка|м.р-н Опочецкий|Псковская область
        # We use REGION field
        loc_parts: list[str] = full_location.strip().rsplit(sep='|', maxsplit=2)  # [point], [rayon], oblast
        self.__store_location_2(loc_parts)

    def __store_location_2(self, loc_name_parts: list[str], parent_idx: bytes = None, names: list = None,
                           row_loc_indexes: list = None):
        if names is None:
            names = list()

        if row_loc_indexes is None:
            row_loc_indexes = list()

        if len(loc_name_parts):
            part_name: str = loc_name_parts.pop().strip()
            names.append(part_name)
            part_index: bytes = self.__my_hash('|'.join(names).encode('utf-8'))
            row_loc_indexes.append(part_index)
            depth = len(row_loc_indexes)
            if not (part_index in self.total_loc_indexes):
                self.total_loc_indexes.append(part_index)
                location = Location(part_index, parent_idx, depth, part_name, row_loc_indexes)
                self.locations.append(location)

            self.__store_location_2(loc_name_parts, part_index, names, indexes)

        return row_loc_indexes[-1]

    @staticmethod
    def __my_hash(_string: bytes) -> bytes:
        return hashlib.md5(_string).digest()

    def store_operator_id_title(self, id: int, title: str) -> None:
        self.op_id_titles.setdefault(id, title)

    def do_action(self, action_name: Optional[str], src_val: str) -> Optional[bool]:
        if action_name is None:
            return None

        if action_name not in self.actions:
            return None

        action_func: Optional[Callable[[str], bool]] = self.actions[action_name]
        if action_func is not None:
            try:
                return action_func(src_val)
            except KeyError:
                if len(src_val) == 0:
                    raise OptionalFieldEmptyException('Пустое значение поля ')
                else:
                    raise KeyError('Не могу выполнить action: Неизвестное значение "%s". ' % src_val)
        else:
            return True


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
