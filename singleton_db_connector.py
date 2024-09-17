import mysql.connector as mysql_conn
import mysql
import app_logger

logger = app_logger.get_logger(__name__)


class DbConnector:
    __instance = None

    def __init__(self, user: str, password: str, database: str, host: str, port: str) -> None:
        if not DbConnector.__instance:
            try:
                DbConnector.__instance = mysql_conn.connect(user=user, password=password, database=database,
                                                            host=host, port=port, connection_timeout=5)
                logger.debug('Connect to DB OK.')
            except mysql.connector.Error as er:
                logger.error(er)
                raise IOError(er)

        else:
            raise Exception('You cannot create another MySQL connection')

    @staticmethod
    def is_connected():
        return bool(DbConnector.__instance)

    @staticmethod
    def get_instance(user: str = "", password: str = "", database: str = "",
                     host="127.0.0.1", port="3306") -> mysql.connector.MySQLConnection:
        if not DbConnector.__instance:
            DbConnector(user, password, database, host, port)
        return DbConnector.__instance

    @staticmethod
    def close_instance() -> None:
        DbConnector.__instance.close()


if __name__ == "__main__":
    credentials = {
        "host": '77.221.200.50',
        "user": 'cdrtest',
        "password": 'LZcoJ4L',
        "database": 'cdrtest',
        "port": "3306"
    }

    # c = DbConnector.get_instance(host='77.221.200.50', user='cdrtest', password='LZcoJ4L', database='cdrtest',)
    c = DbConnector.get_instance(**credentials)
    print(c)
    print(DbConnector.is_connected())
