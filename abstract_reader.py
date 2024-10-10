import logging
import data_types
from url import Url
from abc import abstractmethod
import config

logger = logging.getLogger('logger')


class AbstractReader(object):

    def __init__(self, global_config: config.Config):
        self.config: config.Config = global_config
        self.src_fields: tuple[str, ...] = self.get_src_fields()
        self.url = Url(self.config.get_param_val(r'Src/Url'))
        self.tmp_inp_dir = global_config.get_param_val(r'InputDataDir')

        logger.info("Src Url: %s" % self.url)

    @abstractmethod
    def get_filelist(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_parsed_data(self, skip_header: bool=True) -> data_types.InpData:
        raise NotImplementedError

    @abstractmethod
    def copy_data_to_tmp(self, filename: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self, filename: str) -> data_types.Metadata:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    def get_src_fields(self) -> tuple[str, ...]:
        res = []
        for node in self.config.findall(r'Converter/SrcFields/Field'):
            res.append(self.config.get_param_val(r'Name', node))

        return tuple(res)

    def get_csv_extra_parameters(self) -> dict[str,str]:
        res: dict[str, str] = {}
        for n in self.config.findall(r"Src/CsvExtraParameters/*"):
            k = n.tag
            v = self.config.get_param_val(r".", n)
            res[k] = v

        return res
