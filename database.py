import getpass
import importlib
import os
import sqlite3

from psycopg2.extras import DictCursor

from . import settings
from .base_class import BaseClass
from .helpers import handle_datetimeoffset

PROFILE_DIR = os.environ.get("USERPROFILE", os.path.join("/", "home", getpass.getuser()))


class BaseDBClass(BaseClass):
    """
    This is just the utility class to handle database connection and queries.
    The Objects class wraps around this to map database queries and results to Model objects.
    If you adapt this class, (specify it as a kwarg) and the SQL in Objects.filter, it can work with just about any DBMS.
    """

    debug_queries = False

    default_database = None
    database = None
    database_class = None

    is_ssh_tunnel = False
    ssh_server = None
    ssh_tunnel = False

    server = None
    user = None
    db_file = None
    db_name = None
    port = False

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
        super(BaseDBClass, self).__init__(**kwargs)

        self._debug_handler("Initialize Database Class")

        self.default_database = kwargs.get("database", settings.DATABASE)

        self.db_client = kwargs.get("db_client", self.default_database.get("ENGINE"))

        if isinstance(self.db_client, str):
            self.db_client = importlib.import_module(self.db_client)

        if hasattr(self.db_client, "sqlite_version"):
            self.database_class = "sqlite"
        elif hasattr(self.db_client, "_psycopg"):
            self.database_class = "psql"
        elif hasattr(self.db_client, "__name__"):
            self.database_class = self.db_client.__name__
            if "mssql" in self.database_class:
                self.database_class = "mssql"
            elif "pyodbc" in self.database_class:
                self.database_class = "pyodbc"
        else:
            self._debug_handler("Could not detect database class.")
            self._debug_handler((dir(self.db_client)))

        self._debug_handler("DATABASE CLASS: %s" % self.database_class)

        self.is_ssh_tunnel = kwargs.get("ssh_tunnel", self.default_database.get("SSH_TUNNEL", False))

        if self.is_ssh_tunnel:
            ssh_host = kwargs.get("server", self.default_database.get("HOST"))
            self._init_ssh_tunnel(
                ssh_host,
            )
            self.server = "localhost"
        else:
            self.server = kwargs.get("server", self.default_database.get("HOST"))

        self.user = kwargs.get("user", self.default_database.get("USER"))
        password = kwargs.get("password", self.default_database.get("PASSWORD"))
        self.db_file = kwargs.get("file", self.default_database.get("FILE"))
        self.db_name = kwargs.get("name", self.default_database.get("NAME"))

        if self.port:
            self._debug_handler("Connecting to %s:%i -> %s as %s" % (self.server, self.port, self.db_name, self.user))
        else:
            self._debug_handler("Connecting to %s -> %s as %s" % (self.server, self.db_name, self.user))

        self.database = self.db_name

        if self.database_class == "sqlite":
            self.conn = self.db_client.connect(self.db_file)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()

        elif self.database_class == "psql":
            if self.port:
                self.dsn = "dbname='%s' user='%s' host='%s' port=%i password='%s'" % (
                    self.db_name,
                    self.user,
                    self.server,
                    int(self.port),
                    password,
                )
            else:
                self.dsn = "dbname='%s' user='%s' host='%s' password='%s'" % (
                    self.db_name,
                    self.user,
                    self.server,
                    password,
                )
            self.conn = self.db_client.connect(self.dsn, cursor_factory=DictCursor)
            self.cursor = self.conn.cursor(cursor_factory=DictCursor)

        elif self.database_class == "mssql":
            self.conn = self.db_client.connect(self.server, self.user, password, self.db_name)
            self.cursor = self.conn.cursor(as_dict=True)

        elif self.database_class == "pyodbc":
            self.conn = self.db_client.connect(
                "DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s"
                % (self.server, self.db_name, self.user, password)
            )
            self.conn.add_output_converter(-155, handle_datetimeoffset)
            self.cursor = self.conn.cursor()

        else:
            self.conn = self.db_client.connect(self.server, self.user, password, self.db_name)
            self.cursor = self.conn.cursor(as_dict=True)

        self.standard_cursor = self.conn.cursor()
        self.debug_queries = kwargs.get("debug", False)

        if not self.conn or not self.cursor:
            self._debug_handler("Failed connection.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

        if self.ssh_server:
            self.ssh_server.stop()

        super(BaseDBClass, self).__exit__(exc_type, exc_val, exc_tb)

    # def _init_ssh_tunnel(self, ssh_host, **kwargs):
    #     default_username = os.path.split(PROFILE_DIR)[-1]
    #     default_private_key = os.path.join(PROFILE_DIR, ".ssh", "id_rsa")
    #     ssh_username = kwargs.get("username", self.default_database.get("SSH_USERNAME", default_username))
    #     ssh_password = kwargs.get("password", self.default_database.get("SSH_PASSWORD"))
    #     private_key = kwargs.get("private_key", self.default_database.get("SSH_PRIVATE_KEY", default_private_key))
    #     private_key_passphrase = kwargs.get(
    #         "private_key_passphrase", self.default_database.get("SSH_PRIVATE_KEY_PASSPHRASE", ssh_password)
    #     )
    #     db_port = kwargs.get("db_port", int(self.default_database.get("PORT")))
    #
    #     self._debug_handler("Initiate SSH Connection.")
    #
    #     try:
    #         ssh_params = dict(ssh_username=ssh_username, remote_bind_address=("localhost", db_port),)
    #
    #         if private_key:
    #             ssh_params.update(
    #                 ssh_private_key=private_key, ssh_private_key_password=private_key_passphrase,
    #             )
    #         elif ssh_password:
    #             ssh_params.update(ssh_password=ssh_password)
    #
    #         self.ssh_server = SSHTunnelForwarder((ssh_host, 22), **ssh_params)
    #         self.ssh_server.start()
    #         self.port = int(self.ssh_server.local_bind_port)
    #
    #     except:
    #         self.is_ssh_tunnel = False
    #         self._debug_handler("SSH Connection Failed")
    #     else:
    #         self._debug_handler("SSH Connection Connected: %s:%i" % (self.ssh_server.ssh_host, self.port))

    def _param_string(self):
        if self.database_class in ["psql", "mssql"]:
            self.param_string = "%" + "s"

        else:
            self.param_string = "?"

        return self.param_string

    def _encap_string(self):
        if "mssql" in self.database_class.lower():
            self.encap_left = "["
            self.encap_right = "]"
        elif "mysql" in self.database_class.lower():
            self.encap_left = "`"
            self.encap_right = "`"
        else:
            self.encap_left = '"'
            self.encap_right = '"'

    def encap_string(self, value):
        self._encap_string()

        if isinstance(value, str):
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

        if self.database_class in ["pyodbc"]:
            columns = [column[0] for column in self.cursor.description]
            result = dict(zip(columns, retn))
            retn = result

        return retn

    def _fetch_all(self):
        retn = []

        try:
            retn = self.cursor.fetchall()

        except self.db_client.OperationalError as e:
            self._debug_handler(e)

        if self.database_class in ["pyodbc"]:
            columns = [column[0] for column in self.cursor.description]
            results = []
            for row in retn:
                results.append(dict(zip(columns, row)))
            retn = results

        return retn

    def _db_query(self, query, real_values=False):
        result = None

        if self.database_class == "mssql":
            real_values = tuple(real_values)

        if self.debug_queries:
            self._debug_handler(query)

            if real_values:
                self._debug_handler(real_values)

        try:
            if not real_values:
                result = self.cursor.execute(query)
            else:
                result = self.cursor.execute(query, real_values)

            test_query = query.upper()
            if test_query.startswith("INSERT") or test_query.startswith("UPDATE"):
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
