import sqlite3

from past.builtins import unicode
from psycopg2.extras import DictCursor

import settings
from orm.base_class import BaseClass


class BaseDBClass(BaseClass):
    """
    This is just the utility class to handle database connection and queries.
    The Objects class wraps around this to map database queries and results to Model objects.
    If you adapt this class, (specify it as a kwarg) and the SQL in Objects.filter, it can work with just about any DBMS.
    """

    debug_queries = False

    database = None
    database_class = None

    dsn = None
    db_client = None
    conn = None
    cursor = None

    count = None
    statement = None
    result = None

    encap_left = None
    encap_right = None

    def __init__(self, **kwargs):
        self.db_client = kwargs.get("db_client", settings.DATABASE.get("ENGINE"))

        if hasattr(self.db_client, "sqlite_version"):
            self.database_class = "sqlite"
        elif hasattr(self.db_client, "_psycopg"):
            self.database_class = "psql"
        else:
            print(dir(self.db_client))

        server = kwargs.get("server", settings.DATABASE.get("HOST"))
        user = kwargs.get("user", settings.DATABASE.get("USER"))
        password = kwargs.get("password", settings.DATABASE.get("PASSWORD"))
        db_file = kwargs.get("file", settings.DATABASE.get("FILE"))
        db_name = kwargs.get("name", settings.DATABASE.get("NAME"))

        self.database = db_name

        if self.database_class == "sqlite":
            self.conn = self.db_client.connect(db_file)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()

        elif self.database_class == "psql":
            self.dsn = "dbname='%s' user='%s' host='%s' password='%s'" % (db_name, user, server, password)
            self.conn = self.db_client.connect(self.dsn, cursor_factory=DictCursor)
            self.cursor = self.conn.cursor(cursor_factory=DictCursor)

        else:
            self.conn = self.db_client.connect(server, user, password, db_name)
            self.cursor = self.conn.cursor(as_dict=True)

        self.standard_cursor = self.conn.cursor()
        self.debug_queries = kwargs.pop("debug_queries", False)

        super(BaseDBClass, self).__init__(**kwargs)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

        super(BaseDBClass, self).__exit__(exc_type, exc_val, exc_tb)

    def _param_string(self):
        if self.database_class == "psql":
            self.param_string = "%" + "s"
        else:
            self.param_string = "?"

        return self.param_string

    def _encap_string(self):
        if "mssql" in self.database_class.lower():
            self.encap_left = "["
            self.encap_right = "["
        elif "mysql" in self.database_class.lower():
            self.encap_left = "`"
            self.encap_right = "`"
        else:
            self.encap_left = '"'
            self.encap_right = '"'

    def encap_string(self, value):
        self._encap_string()

        if isinstance(value, (str, unicode)):
            if value[0] != self.encap_left:
                value = "%s%s" % (self.encap_left, value)

            if value[-1] != self.encap_right:
                value = "%s%s" % (value, self.encap_right)

        return value

    def _fetch_one(self):
        retn = {}

        try:
            retn = self.cursor.fetchone()

        except self.db_client.OperationalError:
            pass

        return retn

    def _fetch_all(self):
        retn = []

        try:
            retn = self.cursor.fetchall()

        except self.db_client.OperationalError as e:
            if self.debug_queries:
                self._debug_handler(e)
            pass

        return retn

    def _db_query(self, query, real_values=False):
        result = None

        if self.debug_queries:
            self._debug_handler(query)

            if real_values:
                self._debug_handler(real_values)

        try:
            if not real_values:
                result = self.cursor.execute(query)
            else:
                result = self.cursor.execute(query, real_values)

            self.conn.commit()

        except self.db_client.OperationalError as e:
            query_type = query.split(" ")[0]
            self._debug_handler("%s Query Failed" % query_type)
            self._debug_handler(e)
            self._debug_handler(query)
            if real_values:
                self._debug_handler(real_values)

        except Exception as e:
            query_type = query.split(" ")[0]
            self._debug_handler("%s Query Failed" % query_type)
            self._debug_handler(e)
            self._debug_handler(query)
            if real_values:
                self._debug_handler(real_values)

        return result

    def _db_name(self):

        return self.database
