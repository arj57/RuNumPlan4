import glob
import os.path
import shutil

import app_logger
import config
from overrides import override
from abstract_reader import AbstractReader

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
    def download_data(self, filename: str) -> None:
        # TODO: check permissions
        p_src = os.path.abspath(os.path.basename(filename))
        p_dst = os.path.abspath(self.input_dir)
        shutil.copy2(p_src, p_dst)  # with creation time

    @override()
    def close(self) -> None:
        pass


