from datetime import datetime

import app_logger
import data_types
import utils
from abstract_converter import AbstractConverter
from config import Config
import importlib
import inspect
from abstract_reader import AbstractReader
from data_types import OutData, InpData
from num_plan_converter import NumPlanConverter
from abstract_writer import AbstractWriter
from singleton_git_repo import GitRepo
from url import Url
from git import Repo


logger = app_logger.get_logger(__name__)


# Press the green button in the gutter to run the script.
class RuNumPlan4:
    def __init__(self):
        self.conf = Config()

    @staticmethod
    def reader_factory(config: Config, src_url: Url) -> AbstractReader:
        src_url_scheme: str = src_url.scheme
        mod_name = src_url_scheme + '_reader'
        class_name = src_url_scheme.capitalize() + 'Reader'
        mod = importlib.import_module(mod_name)
        try:
            reader_class = dict(inspect.getmembers(mod, inspect.isclass))[class_name]
        except IndexError:
            raise AttributeError('Не найден класс "%s"' % class_name)
        return reader_class(config, src_url)

    @staticmethod
    def writer_factory(config: Config) -> AbstractWriter:
        dst_url_scheme: str = Url(config.get_param_val(r"Dst/Url")).scheme
        mod_name = dst_url_scheme + '_writer'
        class_name = dst_url_scheme.capitalize() + 'Writer'
        mod = importlib.import_module(mod_name)
        try:
            writer_class = dict(inspect.getmembers(mod, inspect.isclass))[class_name]
        except Exception:
            raise AttributeError('Не найден класс "%s"' % class_name)
        return writer_class(config)

    def process(self) -> None:
        src_data: InpData = self.get_accumulated_inp_data(skip_header=True)
        if self.is_data_changed():
            logger.info("Получены новые данные.")
            converter: AbstractConverter = NumPlanConverter(self.conf)
            dst_data: OutData = converter.get_converted_data(src_data)
            dst: AbstractWriter = self.writer_factory(self.conf)
            dst.store_data(dst_data)
            dst.close()
            self.store_data_to_repo()
            # TODO: store new data to repo
            self.store_data_to_repo()
        else:
            logger.info("Данные не изменились. Нечего сохранять.")

    def get_accumulated_inp_data(self, skip_header: bool=True) -> data_types.InpData:
        meta: list[data_types.Metadata] = []
        records: data_types.InpRecords = []

        src_urls: list[Url] = [Url(i.text) for i in self.conf.findall(r"Src/Urls/Url")]
        for i, url in enumerate(src_urls):
            src: AbstractReader = self.reader_factory(self.conf, url)
            inp: data_types.InpData = src.get_parsed_data(skip_header=(skip_header or i > 0))
            records += inp.records
            meta += inp.metadata
            src.close()
        return data_types.InpData(metadata=meta, records=records)

    def is_data_changed(self) -> bool:
        tmp_inp_dir = self.conf.get_param_val(r'InputDataDir')
        abs_tmp_inp_dir = utils.get_abs_path(tmp_inp_dir)
        repo: Repo = GitRepo.get_instance(abs_tmp_inp_dir)
        return repo.is_dirty()

    def store_data_to_repo(self) -> None:
        logger.info("Сохранение данных в репозитории...")
        tmp_inp_dir = self.conf.get_param_val(r'InputDataDir')
        abs_tmp_inp_dir = utils.get_abs_path(tmp_inp_dir)
        repo: Repo = GitRepo.get_instance(abs_tmp_inp_dir)
        idx = repo.index
        changed_files: list[str] = [item.a_path for item in idx.diff(None)]
        idx.add(changed_files)
        idx.commit(datetime.now().strftime("%Y.%m.%d %H:%M:%S"))
        logger.info("Данные сохранены.")
        # pass


if __name__ == '__main__':
    try:
        num_plan = RuNumPlan4()
        num_plan.process()
    except Exception as msg:
        logger.warning(msg)
        exit(1)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
