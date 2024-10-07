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
    def __init__(self, conf: config.Config) -> None:
        super().__init__(conf)

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

    def _commit(self):
        self.conn.commit()
        logger.info('Commited.')

    def _rollback(self):
        self.conn.rollback()
        logger.info("Rollback.")

    @override()
    def store_data(self, data: data_types.OutData) -> None:
        self.store_rows(data.rows)
        self.store_loc_ref(data.loc_objects)
        self.store_op_ref(data.op_id_titles)
        self.store_metadata()

        if bool(self.config.get_param_val("./DryRun")):
            logger.info("Dry run, not commited")
        else:
            # meta.set_last_row_id()
            # meta.save()
            self._commit()

    def store_rows(self, rows: data_types.OutRows) -> None:
        cursor = self.conn.cursor()
        fields_list: list[str] = self.get_fields_list()
        try:
            ins_query = " INSERT INTO op_data (" + ",".join(fields_list) + ") VALUES(" + (
                    "%s," * len(fields_list)).strip(",") + ")"

            vals = [tuple(row.values()) for row in rows]
            cursor.executemany(ins_query, vals)
        except mysql.connector.Error as er:
            logger.error('Fail to insert CDRs: %s' % er)
            self._rollback()
            raise IOError
        finally:
            cursor.close()

    def store_loc_ref(self, rows: data_types.Locations):
        cursor = self.conn.cursor()
        try:
            cursor.execute("TRUNCATE locations_ref")
            ins_query = "INSERT INTO locations_ref (id, parent_id, level, title) VALUES( %s, %s, %s, %s)"
            vals = list(rows.values())
            cursor.executemany(ins_query, vals)
        except mysql.connector.Error as er:
            logger.error('Fail to insert loc_ref: %s' % er)
            self._rollback()
            raise IOError
        finally:
            cursor.close()

    def store_op_ref(self, rows: data_types.IdTitles):
        cursor = self.conn.cursor()
        try:
            cursor.execute("TRUNCATE operators_ref")
            ins_query = "INSERT INTO operators_ref (inn, title) VALUES( %s, %s)"
            vals = list(rows.items())
            cursor.executemany(ins_query, vals)
        except mysql.connector.Error as er:
            logger.error('Fail to insert loc_ref: %s' % er)
            self._rollback()
            raise IOError
        finally:
            cursor.close()

    def store_metadata(self):
        pass

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
