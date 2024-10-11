import csv
from datetime import datetime
from typing import Optional

from requests import Response
from requests.structures import CaseInsensitiveDict

import app_logger
import config
import requests
from overrides import override
import data_types
from abstract_reader import AbstractReader
from url import Url
from collections.abc import Iterator

logger = app_logger.get_logger(__name__)


class HttpsReader(AbstractReader):
    def __init__(self, global_config: config.Config, src_url: Url):
        super().__init__(global_config, src_url)
        self.response: Optional[Response] = None

    @override()
    def get_parsed_data(self, skip_header: bool=True) -> data_types.InpData:

        while True:
            try:
                req_headers = {'user-agent': r'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/101.0'}
                self.response = requests.get(self.url.string, stream=True, timeout=10, headers=req_headers)
                self.response.encoding = r"utf-8"
                if self.response.status_code == 200:
                    logger.info("Загружаем данные из '%s'..." % self.url.string)
                    lines = self.response.text.splitlines()
                    csv_extra_parameters = self.get_csv_extra_parameters()
                    csv.register_dialect('custom-dialect', **csv_extra_parameters)
                    rec_iter: Iterator = csv.DictReader(lines, fieldnames=self.src_fields, dialect='custom-dialect')
                    if skip_header:
                        next(rec_iter, None)  # skip header

                    records = list(rec_iter)
                    metadata: data_types.Metadata = self._get_metadata(self.response.headers)
                    return data_types.InpData(metadata=[metadata], records=records)
                elif self.response.status_code == 429:
                    raise requests.exceptions.RequestException("Too many reconnects.")
                else:
                    raise requests.exceptions.RequestException("Unhandled status '{}' retrieved.".format(self.response.status_code))
            except requests.exceptions.Timeout:
                pass  # we'll ignore timeout errors and reconnect
            finally:
                self.close()


    def _get_metadata(self, resp_headers: CaseInsensitiveDict[str]) -> data_types.Metadata:
        data_length: int = int(resp_headers["Content-Length"])
        date_str = resp_headers["Last-Modified"]
        try:
            # 'Fri, 11 Oct 2024 00:10:03 GMT'
            dt_modified = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S GMT')
        except ValueError:
            dt_modified = None

        return data_types.Metadata(src_url=self.url.string, created_date=dt_modified, len=data_length)

    @override()
    def copy_data_to_tmp(self) -> None:
        pass

    @override()
    def close(self) -> None:
        if self.response is not None:
            self.response.close()
