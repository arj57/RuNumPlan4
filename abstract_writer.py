# -*- coding: utf-8 -*-
import logging
import typing
import config
import data_types
from url import Url
from abc import abstractmethod

logger = logging.getLogger('logger')


class AbstractWriter(object):
    def __init__(self, global_config: config.Config) -> None:
        logger.debug("%s: preparing parameters...", self.__class__.__name__)
        self.config: config.Config = global_config
        self.url = Url(self.config.get_param_val(r"./Dst/Url"))
        self.fieldslist = self.get_fieldslist()

    @abstractmethod
    def get_filelist(self) -> typing.Optional[list[str]]:
        raise NotImplementedError

    @abstractmethod
    def put_data(self, data2: datatypes.InpData) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    def get_fieldslist(self) -> list[str]:
        res: list[str] = []
        for n in self.config.findall(r"/Converter/DstFields/Field"):
            v = self.config.get_param_val(r"Name", n)
            res.append(v)
        return res

    def get_csv_extra_parameters(self) -> dict[str,str]:
        res: dict[str, str] = {}
        for n in self.config.findall(r"/Dst/CsvExtraParameters/*"):
            k = n.tag
            v = self.config.get_param_val(r".", n)
            res[k] = v
        return res

    # def save_metadata(self, data: str) -> None:
    #     storage_path = self.config.get_param_val(r"/Dst/StorageFilePath")
    #     if storage_path is not None:
    #         logger.debug('Сохраняем имя файла в хранилище "%s".' % storage_path)
    #         with status_storage.StatusStorage(storage_path) as storage:
    #             storage.save(data)

