from abstract_reader import AbstractReader
import app_logger
import config
import csv
import data_types
import os.path
import shutil
from collections.abc import Iterator
from datetime import datetime, timezone
from wcmatch import glob
from wcmatch.fnmatch import BRACE
from overrides import override
from data_types import Metadata

logger = app_logger.get_logger(__name__)


class FileReader(AbstractReader):
    def __init__(self, global_config: config.Config):
        super().__init__(global_config)
        self.basename = os.path.basename(self.url.path)
        self.dir_name = os.path.abspath(os.path.dirname(self.url.path))

        if not self.tmp_inp_dir.startswith('/'):
            self.tmp_inp_dir = os.path.join(os.path.dirname(__file__), 'InputData')

        if self.dir_name == "":
            self.dir_name = r"/"

        logger.info("%s.__init__: File reader path: '%s'" % (self.__class__.__name__, self.dir_name))

    @override()
    def get_filelist(self) -> list[str]:
        # logger.info('Получаем список файлов по пути: "%s"' % (self.url.scheme + r'://' + self.url.path))
        filelist = []
        os.chdir(self.dir_name)
        for entry in glob.glob(self.basename, flags=BRACE):
            filelist.append(entry)
        return filelist

    @override()
    def get_parsed_data(self, skip_header: bool=True) -> data_types.InpData:
        records: data_types.InpRecords = []
        files_metadata: list[data_types.Metadata] = []

        for filename in self.get_filelist():
            self.copy_data_to_tmp(filename)

        os.chdir(self.tmp_inp_dir)
        csv_extra_parameters = self.get_csv_extra_parameters()
        csv.register_dialect('custom-dialect', **csv_extra_parameters)
        for i, filename in enumerate(self.get_filelist()):
            with open(filename, 'r') as fin:
                logger.info("Читаем данные из файла '%s'..." % filename)
                cdr_list = fin.readlines()
            inp: Iterator = csv.DictReader(cdr_list, fieldnames=self.src_fields, dialect='custom-dialect')
            if skip_header or (i > 0):
                next(inp, None)  # skip header
            records += list(inp)
            files_metadata.append(self.get_metadata(filename))

        return data_types.InpData(metadata=files_metadata, records=records)



    @override()
    def copy_data_to_tmp(self, filename: str) -> None:
        # TODO: check permissions
        src_abs_path = os.path.abspath(os.path.basename(filename))
        dst_dir = os.path.abspath(self.tmp_inp_dir)
        shutil.copy2(src_abs_path, dst_dir)  # with creation time

    def get_metadata(self, filename: str) -> Metadata:
        src_abs_path = os.path.abspath(os.path.basename(filename))
        d = datetime.fromtimestamp(os.path.getmtime(src_abs_path), tz=timezone.utc)
        md = Metadata(src_filename=os.path.basename(filename), created_date=d)
        return md

    @override()
    def close(self) -> None:
        pass


