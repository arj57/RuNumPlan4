import app_logger
from config import Config
import importlib
import inspect
from abstract_reader import AbstractReader
from abstract_writer import AbstractWriter
from url import Url


logger = app_logger.get_logger(__name__)


# Press the green button in the gutter to run the script.
class RuNumPlan4:
    def __init__(self):
        self.conf = Config()
        # self.dst: AbstractWriter = self.writer_factory(self.conf)
        self.src: AbstractReader = self.reader_factory(self.conf)
        self.filenames_to_process: list[str] = self.src.get_filelist()

    @staticmethod
    def reader_factory(config: Config) -> AbstractReader:
        src_url_scheme: str = Url(config.get_param_val(r"/Src/Url")).scheme
        mod_name = src_url_scheme + 'reader'
        class_name = src_url_scheme.capitalize() + 'Reader'
        mod = importlib.import_module(mod_name)
        try:
            reader_class = dict(inspect.getmembers(mod, inspect.isclass))[class_name]
        except IndexError:
            raise AttributeError('Не найден класс "%s"' % class_name)
        return reader_class(config)

    # @staticmethod
    # def writer_factory(config: Config) -> AbstractWriter:
    #     dst_url_scheme: str = Url(config.get_param_val(r"/Dst/Url")).scheme
    #     mod_name = dst_url_scheme + 'writer'
    #     class_name = dst_url_scheme.capitalize() + 'Writer'
    #     mod = importlib.import_module(mod_name)
    #     try:
    #         writer_class = dict(inspect.getmembers(mod, inspect.isclass))[class_name]
    #     except Exception:
    #         raise AttributeError('Не найден класс "%s"' % class_name)
    #     return writer_class(config)

    def process(self) -> None:
        for src_filename in self.filenames_to_process:
            self.src.download_data(src_filename)
            # TODO: check if data is fresh
            src_data = self.src.get_parsed_data(src_filename)
            pass


if __name__ == '__main__':
    try:
        num_plan = RuNumPlan4()
        num_plan.process()
    except Exception as msg:
        logger.warning(msg)
        exit(1)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
