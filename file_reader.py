import abstract_reader
from abstract_reader import AbstractReader
import app_logger
import config
import os.path
import shutil
from overrides import override
from url import Url

logger = app_logger.get_logger(__name__)



class FileReader(AbstractReader):
    def __init__(self, global_config: config.Config, src_url: Url):
        super().__init__(global_config, src_url)

    @override()
    def copy_data_to_tmp(self) -> None:
        abs_src_path = abstract_reader.get_abs_path(self.url.path)
        abs_tmp_path = os.path.join(self.abs_tmp_inp_dir, os.path.basename(abs_src_path))
        logger.info("Загружаем данные из '%s' в '%s'..." % (abs_src_path, abs_tmp_path))
        shutil.copy2(abs_src_path, self.abs_tmp_inp_dir)

    @override()
    def close(self) -> None:
        pass