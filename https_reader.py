import os.path
from typing import Optional
from requests import Response
import app_logger
import config
import requests
from overrides import override
from abstract_reader import AbstractReader
from url import Url
from dateutil import parser
logger = app_logger.get_logger(__name__)


def _get_mtime(date_str: str) -> int:
    # 'Fri, 11 Oct 2024 00:10:03 GMT'
    dt = parser.parse(date_str)
    return int(dt.timestamp())


class HttpsReader(AbstractReader):
    def __init__(self, global_config: config.Config, src_url: Url):
        super().__init__(global_config, src_url)
        self.response: Optional[Response] = None

    @override()
    def copy_data_to_tmp(self) -> None:
        while True:
            try:
                req_headers = {'user-agent': r'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/101.0'}
                self.response = requests.get(self.url.string, stream=True, timeout=10, headers=req_headers)
                self.response.encoding = r"utf-8"
                if self.response.status_code == 200:
                    abs_tmp_path = os.path.join(self.abs_tmp_inp_dir, os.path.basename(self.url.path))
                    logger.info("Загружаем данные из '%s' в '%s'..." % (self.url.string, abs_tmp_path))
                    with open(abs_tmp_path, "w") as fout:
                        fout.write(self.response.text)
                    mt = _get_mtime(self.response.headers["Last-Modified"])
                    os.utime(abs_tmp_path, (mt, mt))
                    break
                elif self.response.status_code == 429:
                    raise requests.exceptions.RequestException("Too many reconnects.")
                else:
                    raise requests.exceptions.RequestException("Unhandled status '{}' retrieved.".format(self.response.status_code))
            except requests.exceptions.Timeout:
                pass  # we'll ignore timeout errors and reconnect
            finally:
                self.close()

    @override()
    def close(self) -> None:
        if self.response is not None:
            self.response.close()
