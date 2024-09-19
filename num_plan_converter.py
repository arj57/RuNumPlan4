import typing
from typing import Callable, Optional
import data_types
from abstract_converter import AbstractConverter
from config import Config


class NumPlanConverter(AbstractConverter):
    def __init__(self, config: Config) -> None:
        super().__init__(config)
        # action должна возвращать False, если после action не нужно заполнять поле в dst_row
        self.actions: dict[str, Callable[[str], bool]] = {}

    """
    action для поля выполняет некоторые дополнительные действия, не связанные с заполнением dst_val.
    Например, на основе src_val заполняет какие-то дополнительные структуры данных.
    Если action возвращает False, то значение в dst_val не заносится
    """
    def convert_row(self, src_row: data_types.InpRecord) -> data_types.OutRecord:
        # {'ABC': '800', 'CAPACITY': '10000', 'FROM': '1000000', 'GAR_REGION': 'Российская Федерация', 'INN': '7707049388', 'OPERATOR': 'ПАО "Ростелеком"', 'REGION': 'Российская Федерация', 'TO': '1009999'}
        dst_row: dict[str, typing.Any] = {}
        for dst_field_name in self.dst_fields_metadata_map.keys():
            src_field_name: str = self.dst_fields_metadata_map[dst_field_name].src_field_name
            action_name: Optional[str] = self.dst_fields_metadata_map[dst_field_name].action_name
            src_val: str = src_row[src_field_name]
            try:
                if self.do_action(action_name, src_val) is None or self.do_action(action_name, src_val) == True :
                    dst_row[dst_field_name] = self.typecast_field(src_val, dst_field_name)
            except KeyError as e:
                raise KeyError(str(e) + ' поля "%s"' % src_field_name)
            except OptionalFieldEmptyException as e:
                raise OptionalFieldEmptyException(str(e) + ' поля "%s"' % src_field_name)
        return dst_row

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
