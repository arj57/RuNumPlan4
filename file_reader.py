import glob
import os.path
import shutil
from datetime import datetime, timezone

import app_logger
import config
from overrides import override
from abstract_reader import AbstractReader
from data_types import Metadata

logger = app_logger.get_logger(__name__)


class FileReader(AbstractReader):
    def __init__(self, global_config: config.Config):
        super().__init__(global_config)
        self.basename = os.path.basename(self.url.path)
        self.dir_name = os.path.abspath(os.path.dirname(self.url.path))

        if not self.input_dir.startswith('/'):
            self.input_dir = os.path.join(os.path.dirname(__file__), 'InputData')

        if self.dir_name == "":
            self.dir_name = r"/"

        logger.info("%s.__init__: File reader path: '%s'..." % (self.__class__.__name__, self.dir_name))

    @override()
    def get_filelist(self) -> list[str]:
        logger.info('Получаем список файлов по пути: "%s"...' % (self.url.scheme + r'://' + self.url.path))
        filelist = []
        os.chdir(self.dir_name)
        for entry in glob.glob(self.basename):
            filelist.append(entry)
        return filelist

    @override()
    def download_data_to_tmp(self, filename: str) -> Metadata:
        # TODO: check permissions
        src_abs_path = os.path.abspath(os.path.basename(filename))
        dst_dir = os.path.abspath(self.input_dir)
        shutil.copy2(src_abs_path, dst_dir)  # with creation time

        d = datetime.fromtimestamp(os.path.getctime(src_abs_path), tz=timezone.utc)
        md = Metadata(src_filename=os.path.basename(filename), created_date=d)
        return md

    @override()
    def close(self) -> None:
        pass


