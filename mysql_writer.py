# import typing
# from datetime import datetime

from mysql.connector.cursor import MySQLCursor

import app_logger
import mysql
from overrides import override
import config
import data_types
# from metadata import AbstractMetadata
from singleton_db_connector import DbConnector
from abstract_writer import AbstractWriter

logger = app_logger.get_logger(__name__)


class MysqlWriter(AbstractWriter):
    def __init__(self, config: config.Config) -> None:
        super().__init__(config)
        # self.fields_types: list[str] = self._get_fields_types()

        conn_params = self._get_connection_parameters()
        logger.info('Connecting to host %s DB "%s"...' % (conn_params['host'], conn_params['database']))
        self.conn: mysql.connector.MySQLConnection = DbConnector.get_instance(**conn_params)
        logger.info('Connect to DB OK.')

    def _get_connection_parameters(self) -> data_types.DbConnectionParameters:
        res: data_types.DbConnectionParameters = {
            'host': self.url.hostname,
            'user': self.url.username,
            'password': self.url.password,
            'database': self.url.path.lstrip("/")
        }
        if self.url.port is None:
            res['port'] = 3306
        else:
            res['port'] = self.url.port

        return res

    @override()
    def close(self):
        self.conn.close()
        self.conn = None
        logger.info('Connection to DB closed.')

    def __commit(self):
        self.conn.commit()
        logger.info('Commited.')

    def __rollback(self):
        self.conn.rollback()
        logger.info("Rollback.")

    @override()
    def put_data(self, data2: data_types.OutData) -> None:
        cursor = self.conn.cursor()
        fields_list: list[str] = self.get_fields_list()

        # meta = MysqlMetadata(data2, self.conn)
        try:
            ins_query = " INSERT INTO cdrs (" + ",".join(fields_list) + ") VALUES(" + (
                    "%s," * len(fields_list)).strip(",") + ")"

            cursor.executemany(ins_query, self.__dicts_to_tuples(data2['cdr_records']))
            if bool(self.config.get_param_val("./DryRun")):
                logger.info("Dry run, not commited")
            else:
                # meta.set_last_row_id()
                # meta.save()
                self.__commit()

        except mysql.connector.Error as er:
            logger.error('Fail to insert CDRs: %s' % er)
            self.__rollback()
            raise IOError
        finally:
            cursor.close()

    # def _get_fields_types(self) -> list[str]:
    #     trusted_types = ["int", "datetime"]  # others as needed
    #
    #     res: list[str] = []
    #     for n in self.config.findall(r"/Converter/DstFields/Field"):
    #         t = self.config.get_param_val(r"Type", n)
    #         if (t is None) or (t not in trusted_types):
    #             t = "str"
    #         # t = locate(v)
    #         res.append(t)
    #     return res

    # def __dicts_to_tuples(self, data: data_types.InpRecords) -> list[data_types.TupRec]:
    #     """
    #     Typecasting of each field of record according to config
    #     """

        # r: data_types.Records = data['cdr_records']
        # def record_typecasting(str_rec: data_types.InpRecord) -> data_types.TupRec:
        #     val_list: list = list(str_rec.values())
        #     for i in range(len(val_list)):
        #         # if self.fieldtypes[i] == "str":
        #         #     continue
        #         if val_list[i] is None:
        #             continue
        #         elif val_list[i] == "":
        #             val_list[i] = None
        #         elif self.fields_types[i] == "int":
        #             val_list[i] = int(val_list[i])
        #         elif self.fields_types[i] == "datetime":
        #             val_list[i] = datetime.strptime(val_list[i], '%Y-%m-%d %H:%M:%S')
        #     return tuple(val_list)
        #
        # r2 = list(map(record_typecasting, data))
        # return r2


# class MysqlMetadata(AbstractMetadata):
#     def __init__(self, data: data_types.InpData, connection: mysql.connector.MySQLConnection):
#         super().__init__(data)
#         self.connection = connection
#         self.first_record_id = self.__get_last_row_id() + 1
#         self.last_record_id = None
#
#         logger.debug("first_record_id: %d" % self.first_record_id)
#
#     def __get_last_row_id(self) -> int:
#         cur_3: MySQLCursor = self.connection.cursor()
#         try:
#             cur_3.execute("SELECT MAX(id) FROM cdrs")
#             record_id = cur_3.fetchone()
#             if record_id[0] is None:
#                 return 0
#             else:
#                 return record_id[0]
#         finally:
#             cur_3.close()
#
#     def set_last_row_id(self) -> None:
#         self.last_record_id = self.__get_last_row_id()
#         logger.debug("last_record_id: %d" % self.last_record_id)
#
#     @override()
#     def save(self) -> None:
#         metadata: data_types.TMetadata = self.get_metadata()
#         fields = metadata.keys()
#         vals = tuple(metadata.values())
#         query = ("INSERT INTO metadata (" + ",".join(fields)
#                  + ") VALUES(" + ("%s," * len(fields)).strip(",") + ")")
#
#         cur2: MySQLCursor = self.connection.cursor()
#         try:
#             cur2.execute(query, vals)
#         except mysql.connector.Error:
#             self.connection.rollback()
#             raise IOError
#             # print(traceback.format_exc())
#         finally:
#             cur2.close()

    # @override()
    # def get_metadata(self) -> data_types.TMetadata:
    #     res = super().get_metadata()
    #     res['start_id'] = self.first_record_id
    #     res['end_id'] = self.last_record_id
    #     return res
