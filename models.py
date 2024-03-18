import datetime
import decimal
import json
import re

from dateutil.parser import parse

from .database import BaseDBClass
from .exceptions import FailedToBind, ObjectDoesNotExist, MultipleObjectsReturned
from .helpers import get_val, safe_json_serialize


class Field:
    """
    This class exists to bind a field within your database to a variable with a different name in your model.

    member_id = Field(db_field='MemberID')

    This is also the best way to map values from multiple joined database tables back to a single model variable

    member_id = Field(db_field='MemberID', db_table='Members')
    city = Field(db_table='City', db_table='Addresses')

    """

    value = None
    db_table = None
    db_field = None
    is_function = False
    field_type = ""
    field_data_type = (int, str, float, datetime.date, datetime.datetime, decimal.Decimal)
    max_length = None
    null_field = False
    field_auto_increment = False
    field_default_value = False
    primary_key = False

    def __init__(self, **kwargs):
        self.db_field = kwargs.get("db_field", None)
        self.db_table = kwargs.get("db_table", None)
        self.is_function = kwargs.get("function", False)

        self.field_type = kwargs.get("field_type", None)
        self.max_length = kwargs.get("max_length", None)
        self.null_field = kwargs.get("null", False)
        self.field_auto_increment = kwargs.get("auto_increment", False)
        self.field_default_value = kwargs.get("default", False)
        self.primary_key = kwargs.get("primary_key", False)

    def __str__(self):
        retn = "%s.%s" % (self.db_table, self.db_field)

        if self.value:
            retn = self.value

        return retn

    def set_value(self, value):
        self.value = value
        self.check_value()

    def check_value(self):
        if self.value is not None and not isinstance(self.value, self.field_data_type):
            if hasattr(self, "process_value"):
                self.process_value()
            else:
                raise TypeError(f"Value {self.value}  is not of type {self.field_data_type}.")

    def process_value(self):
        if callable(self.field_data_type):
            try:
                self.value = self.field_data_type(self.value)
            except Exception as e:
                raise TypeError(
                    f"Value {self.value} ({type(self.value)}) cannot be converted to {self.field_data_type} ({e})."
                )
        else:
            print(f"{self.field_data_type} is not callable.")

        raise TypeError(f"Value {self.value} ({type(self.value)}) cannot be converted to {self.field_data_type}.")


class IntegerField(Field):
    def __init__(self, **kwargs):
        super(IntegerField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "INTEGER")
        self.field_data_type = int
        self.max_length = kwargs.get("max_length", 11)

    def __int__(self):
        return int(self.value)

    def process_value(self):
        try:
            self.value = int(self.value)
        except ValueError:
            raise TypeError(f"Value {self.value} ({type(self.value)}) cannot be converted to {self.field_data_type}.")


class BigIntegerField(IntegerField):
    def __init__(self, **kwargs):
        super(BigIntegerField, self).__init__(**kwargs)

    def __int__(self):
        return int(self.value)


class AutoField(IntegerField):
    def __init__(self, **kwargs):
        super(AutoField, self).__init__(**kwargs)
        self.field_auto_increment = True


class BooleanField(IntegerField):
    def __init__(self, **kwargs):
        super(BooleanField, self).__init__(**kwargs)
        self.max_length = kwargs.get("max_length", 1)

    def __bool__(self):
        retn = False
        if self.value:
            retn = True

        return retn


class CharField(Field):
    def __init__(self, **kwargs):
        super(CharField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "VARCHAR")
        self.field_data_type = str
        self.max_length = kwargs.get("max_length", 64)

    # def process_value(self, value):
    #     try:
    #         self.value = str(self.value)
    #     except ValueError:
    #         raise TypeError(f"Value {self.value}  cannot be converted to {self.field_data_type}.")


class BaseDateTimeField(Field):
    def __init__(self, **kwargs):
        super(BaseDateTimeField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "DATETIME")
        self.field_data_type = (datetime.datetime, datetime.date)

    def interpret_date(self, value):
        year = int(value[0:4])
        month = int(value[4:6])
        day = int(value[6:8])

        print(value, year, month, day)

        dt = datetime.datetime(year=year, month=month, day=day)

        return dt.date()

    def interpret_time(self, value):
        hour = int(value[0:2])
        minute = int(value[2:4])
        second = 0

        if len(value) == 6:
            second = int(value[4:6])

        return datetime.time(hour=hour, minute=minute, second=second)


class DateField(BaseDateTimeField):
    def __init__(self, **kwargs):
        super(DateField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "DATE")
        self.field_data_type = datetime.date

    def process_value(self):
        try:
            self.value = self.interpret_date(self.value)
        except ValueError:
            raise TypeError(f"Value {self.value}  cannot be converted to {self.field_data_type}.")


class DateTimeField(BaseDateTimeField):
    def __init__(self, **kwargs):
        super(DateTimeField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "DATETIME")
        self.field_data_type = datetime.datetime

    def process_value(self):
        try:
            self.value = parse(self.value)

        except Exception:
            if isinstance(self.value, str):
                date_value, time_value = self.value.split(" ")
                date_date = self.interpret_date(date_value)
                time_time = self.interpret_time(time_value)

                self.value = datetime.datetime(
                    year=date_date.year,
                    month=date_date.month,
                    day=date_date.day,
                    hour=time_time.hour,
                    minute=time_time.minute,
                    second=time_time.second,
                )


class DecimalField(Field):
    max_digits = None
    decimal_places = None

    def __init__(self, **kwargs):
        super(DecimalField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "DECIMAL")
        self.field_data_type = decimal.Decimal
        self.max_digits = kwargs.get("max_digits", 8)
        self.decimal_places = kwargs.get("decimal_places", 6)
        self.max_length = "%i, %i" % (self.max_digits, self.decimal_places)

    def __float__(self):
        return self.value

    # def process_value(self, value):
    #     try:
    #         self.value = decimal.Decimal(self.value)
    #     except ValueError:
    #         raise TypeError(f"Value {self.value}  cannot be converted to {self.field_data_type}.")


class FloatField(Field):
    def __init__(self, **kwargs):
        super(FloatField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "FLOAT")
        self.field_data_type = float

    def __float__(self):
        return self.value


class TextField(Field):
    def __init__(self, **kwargs):
        super(TextField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "TEXT")
        self.field_data_type = str


class Model:
    """
    This helps to define a model, and bind it to one or more database tables.

    The easiest type of model is just bound to a single database table.

    If the table is already named 'mymodel' in the database (postgres, for example) the db_table in Meta isn't needed.

    class MyModel(Model):
        field_one = None
        field_two = Field(db_field='MyDatabaseField')

        Meta:
            db_table = 'MyTable'

    More complex is a Model defined with one or more joined tables database tables.

    class MyModel(Model):
        field_one = Field(db_field='Field')
        field_two = Field(db_field='MyDatabaseField', db_table='AnotherTable')

        Meta:
            joined = True
            db_table = ['MyTable', 'AnotherTable', 'YetAnotherTable', ...]
            joined_on = 'MyTable.field = AnotherTable.field, MyTable.another_field = YetAnotherTable.field'

    Notice that in the joined_on and db_table variables, I don't specify any AS something... that is handled
    by the Objects class automatically, and will simply convert the table name to a SQL compatible slug for the
    namespace.

    If you haven't defined fields, your result will include the database namespace in it.

    anothertable.Field or yetanothertable.field

    The first table in the db_table list is special, and if a db_table is not specified for a Field, the Object
    mapper will try to map the field to that database table.
    """

    fields = []

    objects = None
    class_slug = None
    class_name = None

    joined = False
    joined_on = ""

    pk = "ID"

    def __init__(self, **kwargs):
        self.class_name = self.__class__.__name__
        self.class_slug = self._db_slug(self.class_name)
        self.db_table = self.class_slug

        for field in dir(self):
            field_attr = getattr(self, field, False)

            if isinstance(field_attr, Field):
                self.fields.append(field)

        meta = getattr(self, "Meta", None)

        if meta:
            self.db_table = getattr(meta, "db_table", False)
            self.pk = getattr(meta, "primary_key", "ID")

            self.joined = getattr(meta, "joined", False)
            self.joined_on = getattr(meta, "join_on", "")

            if hasattr(meta, "database"):
                kwargs["database"] = getattr(meta, "database")

        self.objects = Objects(
            table=self.db_table, model_instance=self, joined=self.joined, joined_on=self.joined_on, **kwargs
        )

    def _db_slug(self, value=None):
        if not value:
            value = self.class_name

        if not isinstance(value, str):
            value = str(value)

        new_value = re.sub("[^a-z0-9-_ ]", "", value.lower()).replace(" ", "_")

        return new_value

    def __str__(self):
        retn = self.class_name

        if hasattr(self, "pk"):
            if hasattr(self, self.pk):
                retn = "%s %s" % (self.class_name, str(getattr(self, self.pk)))

        return retn


class QueryObject:
    container = dict()
    objects_instance = None
    model = None
    pk = None

    def __init__(self, items=False, objects_instance=None, *args, **kwargs):
        self.objects_instance = objects_instance
        self.model = self.objects_instance.model_instance
        self.pk = self.model.pk

        if isinstance(self.pk, str):
            self.pk = "'%s'" % self.pk

        if items:
            self.container = items

        for k, v in list(self.container.items()):
            setattr(self, k, v)

            value_class = getattr(self.model, k)
            value_class.set_value(v)

            self.container.update({k: value_class.value})

    def __str__(self):
        retn = self.container

        if self.container:
            retn = json.dumps(safe_json_serialize(self.container))

        return retn

    def __getattr__(self, item):
        return_value = None

        if item in dir(self.model):
            modelattr = getattr(self.model, item, "FAILED")
            return_value = modelattr(self)

        return return_value

    def __getitem__(self, name):
        return_value = None

        if name in self.container:
            return self.container.get(name)

        return return_value

    def as_dict(self):
        return self.container

    def update(self, **kwargs):
        for field, value in list(kwargs.items()):
            if field in self.container:
                value_class = getattr(self.model, field)
                value_class.set_value(value)

                self.container[field] = value_class.value

        return self

    def delete(self):
        self.objects_instance.delete(self.container)

    def save(self):
        return self.objects_instance.update(**self.container)


class Objects(BaseDBClass):
    """
    This is kind of an ugly hacked out mess...but it works how it is supposed to.

    It only implements filter, and get so far, since that's all I have needed to use up to this point.

    --------------------------------------------------------------------------------------------------------------------
    list_of_objects = Model().objects.filter(**filter_args)

    filter_args is just kwargs, and implements several of the filter suffixes used by Django.
        See _process_filters() for more details. (including the SQL generated)

    There are a few keyword arguments that can be passed in with your filter_args, that are removed and processed before
    the query is generated.

        return_dicts   - (Boolean) Rather than returning your query results as a list of Objects, they will be
                         returned as a list of dicts.
        return_set     - (Boolean) This will cause your database resulds to be returned as a list of values
                         from a single column. This is poorly named, and should really be values_list, since it
                         actually returns a list rather than a set.
        return_set_key - (String) This specified the column name for the results in return_set.

    --------------------------------------------------------------------------------------------------------------------
    object = Model().objects.get(**filter_args)

    Works exactly like filter, but returns a single object rather than a list of objects.
    If more than one Object is found, a MultipleObjectsReturned exception is raised.

    """

    table = None
    model_instance = None

    joined = False
    joined_on = ""
    join_where = None

    tables = []
    table_namespaces = dict()
    table_namespaces_lookup = dict()
    columns = ["*"]
    column_lookup = dict()
    column_lookup_reverse = dict()

    where_values = []

    table_definition = []
    db_values = None

    parametrized = True

    def __init__(self, **kwargs):
        super(Objects, self).__init__(**kwargs)
        self.table = kwargs.pop("table")
        self.model_instance = kwargs.pop("model_instance", None)

        self.joined = kwargs.pop("joined", False)
        self.joined_on = kwargs.pop("joined_on", "")

        self.table_definition = []

        defined_fields = []
        for field in dir(self.model_instance):
            field_attr = getattr(self.model_instance, field, False)

            if isinstance(field_attr, Field):
                defined_fields.append(field)

        if self.joined:
            self.tables = self.table
            self._init_join()

        has_pk = False
        pk_name = "id"
        if len(defined_fields) > 0:
            self.columns = []

            for attr_name in defined_fields:
                attr = getattr(self.model_instance, attr_name, False)

                attr_is_function = get_val(attr, "is_function", False)

                attr_db_table = get_val(attr, "db_table", False)
                attr_real_field = get_val(attr, "db_field", attr_name)
                field_definition = get_val(attr, "field_type", "TEXT")
                field_length = get_val(attr, "max_length", None)
                field_allow_null = get_val(attr, "null_field", False)
                field_auto_increment = get_val(attr, "auto_increment", False)
                field_default_value = get_val(attr, "default", False)

                field_definition, has_length = self._process_data_type(field_definition, field_length)

                if self.database_class == "sqlite":
                    if "DATE" in field_definition:
                        field_definition = "TEXT"
                    elif "DECIMAL" in field_definition:
                        field_definition = "REAL"

                if not attr_is_function:
                    if attr_db_table and len(self.tables) > 0:
                        real_column = "%s.%s" % (
                            self.table_namespaces_lookup.get(attr_db_table),
                            self.encap_string(attr_real_field),
                        )
                    else:
                        real_column = self.encap_string(attr_real_field)
                else:
                    real_column = attr_real_field

                if attr_real_field == pk_name or get_val(attr, "primary_key", False):
                    if self.database_class == "sqlite":
                        field_definition = "%s PRIMARY KEY" % field_definition
                    pk_name = real_column
                    has_pk = True

                tabledef = "%s %s" % (real_column, field_definition)

                if has_length:
                    tabledef = "%s (%s)" % (tabledef, str(field_length))

                if not field_allow_null:
                    tabledef = "%s NOT NULL" % tabledef

                if field_auto_increment:
                    if self.database_class == "sqlite":
                        tabledef = "%s AUTOINCREMENT" % tabledef
                    elif self.database_class == "psql":
                        tabledef = "%s SERIAL" % tabledef
                    else:
                        tabledef = "%s AUTO_INCREMENT" % tabledef

                if field_default_value:
                    tabledef = "%s DEFAULT '%s'" % (tabledef, field_default_value)

                self.table_definition.append(tabledef)
                column_name = "%s AS %s" % (real_column, attr_name)

                self.column_lookup[attr_name] = real_column
                self.column_lookup_reverse[real_column] = attr_name
                self.columns.append(column_name)

        if not has_pk:
            if self.database_class == "sqlite":
                self.table_definition.append("%s BIGINT(20) NOT NULL PRIMARY KEY" % self.encap_string(pk_name))
            elif self.database_class == "psql":
                self.table_definition.append("%s SERIAL PRIMARY KEY" % (pk_name))
            else:
                self.table_definition.append("%s BIGINT(20) NOT NULL AUTO_INCREMENT" % self.encap_string(pk_name))

        if self.database_class not in ("sqlite", "psql"):
            self.table_definition.append("KEY(%s)" % self.encap_string(pk_name))

        if not self.table or not self.model_instance:
            raise FailedToBind("You must pass in a table and the model instance.")

        # if self.debug:
        #     self._debug_handler(self.column_lookup, pretty=True)
        #     self._debug_handler(self.column_lookup_reverse, pretty=True)

    def _init_join(self):
        join_strings = []
        join_on = self.joined_on

        for table_name in self.table:
            namespace_key = re.sub("[^a-z]", "", table_name.lower())

            self.table_namespaces.update({namespace_key: table_name})
            self.table_namespaces_lookup.update({table_name: namespace_key})
            if self.database_class == "mssql" or self.database_class == "pyodbc":
                join_strings.append("%s %s" % (table_name, namespace_key))
            else:
                join_strings.append("%s.%s %s" % (self.database, table_name, namespace_key))

            join_on = join_on.replace(table_name, namespace_key).replace(",", " AND ")

        join_string = ", ".join(join_strings)

        self.join_where = join_on
        self.table = join_string

    def _build_query(self, **kwargs):
        columns = kwargs.get("columns", self.columns)
        where = kwargs.get("where", False)
        order_by = kwargs.get("order_by", False)
        limit = kwargs.get("limit", False)

        if self.debug_queries:
            self._debug_handler("SELECT: %s" % ", ".join(columns))
            self._debug_handler("FROM: %s" % self.table)
            if where:
                self._debug_handler("WHERE: %s" % where)
            if order_by:
                self._debug_handler("ORDER BY: %s" % order_by)
            if limit:
                self._debug_handler("LIMIT: %s" % str(limit))
            self._debug_handler("-" * 80)

        if limit and ("mssql" in self.database_class.lower() or "pyodbc" in self.database_class.lower()):
            query = "SELECT TOP (%i) %s FROM %s" % (limit, ",".join(columns), self.table)

        else:
            query = "SELECT %s FROM %s" % (",".join(columns), self.table)

        if where:
            query = "%s WHERE %s" % (query, where)

        if self.join_where:
            if where:
                query = "%s AND %s" % (query, self.join_where)

            else:
                query = "%s WHERE %s" % (query, self.join_where)

        if order_by:
            query = "%s ORDER BY %s" % (query, order_by)

        if limit and "mssql" not in self.database_class.lower() and "pyodbc" not in self.database_class.lower():
            query = "%s LIMIT %i" % (query, limit)

        query = "%s;" % query

        return query

    def _process_data_type(self, data_type, length=False):
        affinity = data_type
        has_length = False

        if self.database_class == "sqlite":
            has_length = False
            if data_type in (
                "INT",
                "INTEGER",
                "TINYINT",
                "SMALLINT",
                "MEDIUMINT",
                "BIGINT",
                "UNSIGNED BIG INT",
                "INT2",
                "INT8",
            ):
                affinity = "INTEGER"

            elif data_type in (
                "CHARACTER",
                "VARCHAR",
                "VARYING CHARACTER",
                "CHARACTER VARYING",
                "NCHAR",
                "NATIVE CHARACTER",
                "NVARCHAR",
                "TEXT",
                "CLOB",
            ):
                affinity = "TEXT"

            elif data_type in ("REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT"):
                affinity = "REAL"

            elif data_type in ("NUMERIC", "DECIMAL", "BOOLEAN", "DATE", "DATETIME"):
                affinity = "NUMERIC"

        if self.database_class == "psql":
            if data_type in (
                "INT",
                "INTEGER",
                "TINYINT",
                "SMALLINT",
                "MEDIUMINT",
                "BIGINT",
                "UNSIGNED BIG INT",
                "INT2",
                "INT8",
            ):
                has_length = False

            elif data_type in (
                "CHARACTER",
                "VARCHAR",
                "VARYING CHARACTER",
                "CHARACTER VARYING",
                "NCHAR",
                "NATIVE CHARACTER",
                "NVARCHAR",
            ):
                has_length = True

            elif data_type in ("TEXT", "CLOB", "BLOB"):
                has_length = False

        return affinity, has_length

    def _process_filters(self, **kwargs):
        wheres = []

        real_values = []
        for k, v in list(kwargs.items()):
            if v is not None:
                key_parts = k.split("__")
                key = key_parts[0]
                key_function = key_parts[1] if len(key_parts) > 1 else None
                key_operator = key_parts[2] if len(key_parts) > 2 else "and"

                if key_function not in ["iexact", "icontains", "istartswith", "iendswith", "contains"]:
                    self.where_values.append(v)

                # If a Field is defined on the model, we translate it.
                key = self.column_lookup.get(key, key)

                if key_function == "iexact":
                    appendval = v.upper()
                    if not self.parametrized:
                        where_append = "UPPER(%s) = '%s'" % (str(key), appendval)
                    else:
                        where_append = "UPPER(%s) = %s" % (str(key), self._param_string())
                        self.where_values.append(appendval)
                elif key_function == "icontains":
                    appendval = "%" + v.upper() + "%"
                    if not self.parametrized:
                        where_append = "UPPER(%s) LIKE '%s'" % (str(key), appendval)
                    else:
                        where_append = "UPPER(%s) LIKE %s" % (str(key), self._param_string())
                        self.where_values.append(appendval)
                elif key_function == "contains":
                    appendval = "%" + v + "%"
                    if not self.parametrized:
                        where_append = "%s LIKE '%s'" % (str(key), appendval)
                    else:
                        where_append = "%s LIKE %s" % (str(key), self._param_string())
                        self.where_values.append(appendval)
                elif key_function == "startswith":  # Seems *slightly* faster than LIKE '...%'
                    if not self.parametrized:
                        where_append = "LEFT(%s, %i) = '%s'" % (str(key), len(str(v)), v)
                    else:
                        where_append = "LEFT(%s, %i) = %s" % (str(key), len(str(v)), self._param_string())
                elif key_function == "endswith":
                    if not self.parametrized:
                        where_append = "RIGHT(%s, %i) = '%s'" % (str(key), len(str(v)), self._param_string())
                    else:
                        where_append = "RIGHT(%s, %i) = %s" % (str(key), len(str(v)), v)
                elif key_function == "iendswith":
                    appendval = v.upper()
                    if not self.parametrized:
                        where_append = "UPPER(RIGHT(%s, %i)) = '%s'" % (str(key), len(str(v)), self._param_string())
                        self.where_values.append(appendval)
                    else:
                        where_append = "UPPER(RIGHT(%s, %i)) = %s" % (str(key), len(str(v)), appendval)
                elif key_function == "istartswith":
                    appendval = v.upper()
                    if not self.parametrized:
                        where_append = "UPPER(LEFT(%s, %i)) = '%s'" % (str(key), len(str(v)), appendval)
                    else:
                        where_append = "UPPER(LEFT(%s, %i)) = %s" % (str(key), len(str(v)), self._param_string())
                        self.where_values.append(appendval)
                elif key_function == "length_lt":
                    where_append = "LENGTH(%s) < %s" % (str(key), self._param_string())
                    self.where_values.append(v)
                elif key_function == "length_lte":
                    where_append = "LENGTH(%s) <= %s" % (str(key), self._param_string())
                    self.where_values.append(v)
                elif key_function == "length_gt":
                    where_append = "LENGTH(%s) > %s" % (str(key), self._param_string())
                    self.where_values.append(v)
                elif key_function == "length_gte":
                    where_append = "LENGTH(%s) >= %s" % (str(key), self._param_string())
                    self.where_values.append(v)
                elif key_function == "iendswith":
                    appendval = v.upper()
                    if not self.parametrized:
                        where_append = "UPPER(RIGHT(%s, %i)) = '%s'" % (str(key), len(str(v)), appendval)
                    else:
                        where_append = "UPPER(RIGHT(%s, %i)) = %s" % (str(key), len(str(v)), self._param_string())
                        self.where_values.append(appendval)
                elif key_function == "not_like":
                    if not self.parametrized:
                        where_append = "%s NOT LIKE '%s'" % (str(key), v)
                    else:
                        where_append = "%s NOT LIKE %s" % (str(key), self._param_string())
                elif key_function == "isnull":
                    comparison = "IS NOT" if not v else "IS"
                    where_append = "%s %s NULL" % (str(key), comparison)
                elif key_function == "lt":
                    if not self.parametrized:
                        if isinstance(v, str):
                            where_append = "%s < '%s'" % (str(key), v)
                        else:
                            where_append = "%s < %s" % (str(key), v)
                    else:
                        where_append = "%s < %s" % (str(key), self._param_string())
                elif key_function == "lte":
                    if not self.parametrized:
                        if isinstance(v, str):
                            where_append = "%s <= '%s'" % (str(key), v)
                        else:
                            where_append = "%s <= %s" % (str(key), v)
                    else:
                        where_append = "%s <= %s" % (str(key), self._param_string())
                elif key_function == "gt":
                    if not self.parametrized:
                        if isinstance(v, str):
                            where_append = "%s > '%s'" % (str(key), v)
                        else:
                            where_append = "%s > %s" % (str(key), v)
                    else:
                        where_append = "%s > %s" % (str(key), self._param_string())
                elif key_function == "gte":
                    if not self.parametrized:
                        if isinstance(v, str):
                            where_append = "%s >= '%s'" % (str(key), v)
                        else:
                            where_append = "%s >= %s" % (str(key), v)
                    else:
                        where_append = "%s >= %s" % (str(key), self._param_string())
                elif key_function == "in":
                    if not self.parametrized:
                        v_val = v
                        if isinstance(v, list):
                            v_val = str(tuple(v))
                        where_append = "%s IN %s" % (str(key), v_val)
                    else:
                        where_append = "%s IN %s" % (str(key), self._param_string())
                elif key_function == "not_in":
                    if not self.parametrized:
                        v_val = v
                        if isinstance(v, list):
                            v_val = str(tuple(v))
                        where_append = "%s NOT IN %s" % (str(key), v_val)
                    else:
                        where_append = "%s NOT IN %s" % (str(key), self._param_string())
                else:
                    if not self.parametrized:
                        if isinstance(v, str):
                            where_append = "%s = '%s'" % (str(key), v)
                        else:
                            where_append = "%s = %s" % (str(key), v)
                    else:
                        where_append = "%s = %s" % (str(key), self._param_string())

                where_string = ""

                if key_operator:
                    key_operator_parts = key_operator.split("_")
                    operator_length = len(key_operator_parts)

                    if operator_length > 0:
                        key_operator = key_operator_parts[0].upper()

                    second_key_operator = None
                    if operator_length > 1:
                        second_key_operator = key_operator_parts[1].upper()

                    key_operator_action = "START"
                    if operator_length > 2:
                        key_operator_action = key_operator_parts[2].upper()

                    if len(wheres) > 0:
                        if operator_length == 1:
                            where_string = "%s %s" % (key_operator, where_append)
                        elif operator_length == 2:
                            where_string = "%s (%s" % (key_operator, where_append)
                        elif operator_length == 3 and key_operator_action == "END":
                            where_string = "%s %s)" % (second_key_operator, where_append)
                        where_append = ""

                where_string = "%s %s".strip() % (where_string, where_append)
                wheres.append(where_string)

        where_return = " ".join(wheres).replace("  ", " ").strip()

        return where_return

    def create_table(self):
        query = "CREATE TABLE %s (\n%s\n);" % (self.encap_string(self.table), ",\n".join(self.table_definition))
        self._db_query(query)

    def drop_table(self):
        query = "DROP TABLE %s;" % self.encap_string(self.table)
        self._db_query(query)

    def truncate_table(self):
        query = "TRUNCATE TABLE %s;" % self.encap_string(self.table)
        self._db_query(query)

    def create(self, **kwargs):
        query_parts = ["INSERT INTO", self.encap_string(self.table)]

        insert_fields = []
        real_insert_values = []
        insert_values = []
        for field, value in list(kwargs.items()):
            real_column = self.column_lookup.get(field, field)
            insert_fields.append(self.encap_string(real_column))
            if isinstance(value, list):
                value = json.dumps(value)
            real_insert_values.append(value)
            insert_values.append(self._param_string())

        query_parts.append("(%s)" % ",".join(insert_fields))
        query_parts.append("VALUES")
        query_parts.append("(%s)" % ",".join(insert_values))

        query = "%s;" % " ".join(query_parts)

        self._db_query(query, real_insert_values)

        # return self.get(**kwargs)

    def update(self, **fields):
        query_parts = ["UPDATE", self.encap_string(self.table), "SET"]

        update_values = []
        real_insert_values = []
        for field, value in list(fields.items()):
            real_column = self.column_lookup.get(field, field)
            update_values.append("%s=%s" % (self.encap_string(real_column), self._param_string()))
            if isinstance(value, list):
                value = json.dumps(value)
            real_insert_values.append(value)

        query_parts.append(",".join(update_values))
        query_parts.append("WHERE")
        # query_parts.append("%s=%s" % (self.encap_string(self.model_instance.pk), fields.get(self.model_instance.pk)))
        query_parts.append(self.encap_string(self.model_instance.pk) + "=%s")
        real_insert_values.append(fields.get(self.model_instance.pk))

        query = "%s;" % " ".join(query_parts)

        self._db_query(query, real_insert_values)

        get_params = {self.model_instance.pk: fields.get(self.model_instance.pk)}

        return self.get(**get_params)

    def delete(self, **fields):
        query_parts = [
            "DELETE FROM",
            self.encap_string(self.table),
            "WHERE",
            "%s=%s" % (self.encap_string(self.model_instance.pk), fields.get(self.model_instance.pk)),
        ]

        query = "%s;" % " ".join(query_parts)

        self._db_query(query)

    def query_raw(self, query, **kwargs):
        return_dicts = kwargs.pop("return_dicts", False)
        return_set = kwargs.pop("return_set", False)
        return_set_key = kwargs.pop("return_set_key", None)

        filter_result = []

        try:
            self._debug_handler(query)
        except:
            self._debug_handler(query)

        else:
            query_results = self._fetch_all()
            for query_result in query_results:
                if return_set and return_set_key:
                    filter_result.append(query_result.get(return_set_key))

                elif return_dicts:
                    filter_result.append(query_result)

                else:
                    filter_result.append(QueryObject(query_result, self))

        return filter_result

    def filter(self, **kwargs):
        return_dicts = kwargs.pop("return_dicts", False)
        return_set = kwargs.pop("return_set", False)
        return_set_key = kwargs.pop("return_set_key", None)
        result_limit = kwargs.pop("result_limit", False)
        order_by = kwargs.pop("order_by", False)
        select_all = kwargs.pop("select_all", False)
        columns = kwargs.pop("columns", False)
        self.parametrized = kwargs.pop("parametrized", True)

        if not columns:
            columns = self.columns

        else:
            print(self.columns)
            print(columns)

        filter_result = []
        self.where_values = []

        if not select_all:
            where = self._process_filters(**kwargs)
            query = self._build_query(columns=columns, where=where, limit=result_limit, order_by=order_by)

        else:
            query = self._build_query(columns=columns, limit=result_limit, order_by=order_by)

        # print(query)

        # self.debug = True
        # self.debug_queries = True
        # self.debug_stdout = True

        try:
            if not self.parametrized:
                self._db_query(query)
            else:
                self._db_query(query, self.where_values)

        except:
            self._debug_handler(query)

        else:
            query_results = self._fetch_all()
            for query_result in query_results:
                if return_set and return_set_key:
                    filter_result.append(query_result.get(return_set_key))

                elif return_dicts:
                    filter_result.append(query_result)

                else:
                    filter_result.append(QueryObject(query_result, self))

        return filter_result

    def get(self, **kwargs):
        query_results = self.filter(**kwargs)

        if len(query_results) == 0:
            raise ObjectDoesNotExist

        elif len(query_results) > 1:
            raise MultipleObjectsReturned

        else:
            return query_results[0]

    def all(self, **kwargs):
        kwargs.update(dict(select_all=True))
        query_results = self.filter(**kwargs)

        return query_results

    def raw_query(self, query, **kwargs):
        return_dicts = kwargs.pop("return_dicts", False)
        return_set = kwargs.pop("return_set", False)
        return_set_key = kwargs.pop("return_set_key", None)

        filter_result = []

        self.debug_queries = True

        try:
            self._db_query(query)

        except:
            self._debug_handler(query)

        else:
            query_results = self._fetch_all()

            for query_result in query_results:
                if return_set and return_set_key:
                    filter_result.append(query_result.get(return_set_key))

                elif return_dicts:
                    filter_result.append(query_result)

                else:
                    filter_result.append(QueryObject(query_result, self))

        return filter_result
