from mysql.connector.cursor import MySQLCursor

import app_logger
import mysql
from overrides import override
import config
import data_types
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

    @override()
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
        self.store_metadata(data.metadata)

        if bool(self.config.get_param_val("./DryRun")):
            logger.info("Dry run, not commited")
        else:
            self._commit()

    def store_rows(self, rows: data_types.OutRows) -> None:
        logger.info("Storing rows...")
        cursor = self.conn.cursor()
        fields_list: list[str] = self.get_fields_list()
        try:
            cursor.execute("TRUNCATE op_data")
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
        logger.info("Storing locations references...")
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
        logger.info("Storing operators references...")
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

    def store_metadata(self, meta: list[data_types.Metadata]):
        logger.info("Storing metadata...")
        cursor = self.conn.cursor()
        try:
            ins_query = "INSERT INTO metadata (filename, dt_created) VALUES( %s, %s)"
            cursor.executemany(ins_query, meta)
        except mysql.connector.Error as er:
            logger.error('Fail to insert loc_ref: %s' % er)
            self._rollback()
            raise IOError
        finally:
            cursor.close()
