import json
import re

from orm.database import BaseDBClass
from orm.exceptions import FailedToBind, ObjectDoesNotExist, MultipleObjectsReturned
from orm.helpers import get_val


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
    real_field = None
    field_type = ""
    max_length = None
    null_field = False
    field_auto_increment = False
    field_default_value = False
    primary_key = False

    def __init__(self, **kwargs):
        self.real_field = kwargs.get("db_field", None)
        self.db_table = kwargs.get("db_table", None)

        self.field_type = kwargs.get("field_type", None)
        self.max_length = kwargs.get("max_length", None)
        self.null_field = kwargs.get("null", False)
        self.field_auto_increment = kwargs.get("auto_increment", False)
        self.field_default_value = kwargs.get("default", False)
        self.primary_key = kwargs.get("primary_key", False)

    def __str__(self):
        retn = "%s.%s" % (self.db_table, self.real_field)

        if self.value:
            retn = self.value

        return retn


class IntegerField(Field):
    def __init__(self, **kwargs):
        super(IntegerField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "INTEGER")
        self.max_length = kwargs.get("max_length", 11)


class AutoField(IntegerField):
    def __init__(self, **kwargs):
        super(AutoField, self).__init__(**kwargs)
        self.field_auto_increment = True


class BooleanField(IntegerField):
    def __init__(self, **kwargs):
        super(BooleanField, self).__init__(**kwargs)
        self.max_length = kwargs.get("max_length", 1)


class CharField(Field):
    def __init__(self, **kwargs):
        super(CharField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "VARCHAR")
        self.max_length = kwargs.get("max_length", 64)


class DateField(Field):
    def __init__(self, **kwargs):
        super(DateField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "DATE")


class DateTimeField(Field):
    def __init__(self, **kwargs):
        super(DateTimeField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "DATETIME")


class DecimalField(Field):
    max_digits = None
    decimal_places = None

    def __init__(self, **kwargs):
        super(DecimalField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "DECIMAL")
        self.max_digits = kwargs.get("max_digits", 8)
        self.decimal_places = kwargs.get("decimal_places", 6)
        self.max_length = "%i, %i" % (self.max_digits, self.decimal_places)


class FloatField(Field):
    def __init__(self, **kwargs):
        super(FloatField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "FLOAT")


class TextField(Field):
    def __init__(self, **kwargs):
        super(TextField, self).__init__(**kwargs)
        self.field_type = kwargs.get("field_type", "TEXT")


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

        meta = getattr(self, "Meta", None)

        if meta:
            self.db_table = getattr(meta, "db_table", False)
            self.pk = getattr(meta, "primary_key", "ID")

            self.joined = getattr(meta, "joined", False)
            self.joined_on = getattr(meta, "join_on", "")

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
    pk = None

    def __init__(self, items=False, objects_instance=None):
        self.objects_instance = objects_instance
        self.pk = self.objects_instance.model_instance.pk
        if items:
            self.container = items

        for k, v in self.container.items():
            setattr(self, k, v)

    def __str__(self):

        return json.dumps(self.container)

    def __getitem__(self, name):

        return self.container.get(name)

    def update(self, **kwargs):
        for field, value in kwargs.items():
            if field in self.container:
                self.container[field] = value

        return self

    def delete(self):

        self.objects_instance.delete(self.container)

    def save(self):

        return self.objects_instance.update(self.container)


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

    table_definition = []
    db_values = None

    def __init__(self, **kwargs):
        self.table = kwargs.pop("table")
        self.model_instance = kwargs.pop("model_instance", None)

        self.joined = kwargs.pop("joined", False)
        self.joined_on = kwargs.pop("joined_on", "")

        self.table_definition = []

        super(Objects, self).__init__(**kwargs)

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

                if attr_db_table and len(self.tables) > 0:
                    real_column = "%s.%s" % (
                        self.table_namespaces_lookup.get(attr_db_table),
                        self.encap_string(attr_real_field),
                    )
                else:
                    real_column = self.encap_string(attr_real_field)

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
                self.table_definition.append(
                    "%s SERIAL PRIMARY KEY" % (pk_name)
                )
            else:
                self.table_definition.append("%s BIGINT(20) NOT NULL AUTO_INCREMENT" % self.encap_string(pk_name))

        if self.database_class not in ("sqlite", "psql"):
            self.table_definition.append("KEY(%s)" % self.encap_string(pk_name))

        if not self.table or not self.model_instance:
            raise FailedToBind("You must pass in a table and the model instance.")

    def _init_join(self):
        join_strings = []
        join_on = self.joined_on

        for table_name in self.table:
            namespace_key = re.sub("[^a-z]", "", table_name.lower())
            self.table_namespaces.update({namespace_key: table_name})
            self.table_namespaces_lookup.update({table_name: namespace_key})
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

        if limit and "mssql" in self.database_class.lower():
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

        if limit and "mssql" not in self.database_class.lower():
            query = "%s LIMIT %i" % (query, limit)

        query = "%s;" % query

        return query

    def _process_data_type(self, data_type, length=False):
        affinity = data_type
        has_length = False

        if self.database_class == "sqlite":
            has_length = False
            if data_type in ("INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT", "UNSIGNED BIG INT", "INT2", "INT8"):
                affinity = "INTEGER"

            elif data_type in ("CHARACTER" ,"VARCHAR", "VARYING CHARACTER", "CHARACTER VARYING", "NCHAR", "NATIVE CHARACTER", "NVARCHAR", "TEXT", "CLOB"):
                affinity = "TEXT"

            elif data_type in ("REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT"):
                affinity = "REAL"

            elif data_type in ("NUMERIC", "DECIMAL", "BOOLEAN", "DATE", "DATETIME"):
                affinity = "NUMERIC"

        if self.database_class == "psql":
            if data_type in ("INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT", "UNSIGNED BIG INT", "INT2", "INT8"):
                has_length = False

            elif data_type in ("CHARACTER" ,"VARCHAR", "VARYING CHARACTER", "CHARACTER VARYING", "NCHAR", "NATIVE CHARACTER", "NVARCHAR",):
                has_length = True

            elif data_type in ("TEXT", "CLOB", "BLOB"):
                has_length = False

        return affinity, has_length

    def _process_filters(self, **kwargs):
        wheres = []

        real_values = []
        for k, v in kwargs.items():
            key_parts = k.split("__")
            key = key_parts[0]
            key_function = key_parts[1] if len(key_parts) > 1 else None
            key_operator = key_parts[2] if len(key_parts) > 2 else "and"
            real_values.append(v)

            # If a Field is defined on the model, we translate it.
            key = self.column_lookup.get(key, key)

            if key_function == "iexact":
                where_append = "UPPER(%s) = %s" % (str(key), self._param_string())
            elif key_function == "icontains":
                where_append = "UPPER(%s) LIKE %%%s%%" % (str(key), self._param_string())
            elif key_function == "contains":
                where_append = "%s LIKE %%%s%%" % (str(key), self._param_string())
            elif key_function == "startswith":  # Seems *slightly* faster than LIKE '...%'
                where_append = "LEFT(%s, %i) = %s" % (str(key), len(str(v)), self._param_string())
            elif key_function == "endswith":
                where_append = "RIGHT(%s, %i) = %s" % (str(key), len(str(v)), self._param_string())
            elif key_function == "istartswith":
                where_append = "UPPER(LEFT(%s, %i)) = %s" % (str(key), len(str(v)), self._param_string())
            elif key_function == "iendswith":
                where_append = "UPPER(RIGHT(%s, %i)) = " % (str(key), len(str(v)), self._param_string())
            elif key_function == "not_like":
                where_append = "%s NOT LIKE %s" % (str(key), self._param_string())
            elif key_function == "isnull":
                comparison = "IS NOT" if not v else "IS"
                where_append = "%s %s NULL" % (str(key), comparison)
            elif key_function == "lt":
                where_append = "%s < %s" % (str(key), self._param_string())
            elif key_function == "lte":
                where_append = "%s <= %s" % (str(key), self._param_string())
            elif key_function == "gt":
                where_append = "%s > %s" % (str(key), self._param_string())
            elif key_function == "gte":
                where_append = "%s >= %s" % (str(key), self._param_string())
            elif key_function == "in":
                where_append = "%s IN %s" % (str(key), self._param_string())
            elif key_function == "not_in":
                where_append = "%s NOT IN %s" % (str(key), self._param_string())
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
        for field, value in kwargs.items():
            real_column = self.column_lookup.get(field, field)
            insert_fields.append(self.encap_string(real_column))
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
        for field, value in fields.items():
            real_column = self.column_lookup.get(field, field)
            update_values.append("%s=%s" % (self.encap_string(real_column)), self._param_string())
            real_insert_values.append(value)

        query_parts.append(",".join(update_values))
        query_parts.append("WHERE")
        query_parts.append("%s=%s" % (self.encap_string(self.model_instance.pk), fields.get(self.model_instance.pk)))

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

    def filter(self, **kwargs):
        return_dicts = kwargs.pop("return_dicts", False)
        return_set = kwargs.pop("return_set", False)
        return_set_key = kwargs.pop("return_set_key", None)
        result_limit = kwargs.pop("result_limit", False)
        order_by = kwargs.pop("order_by", False)
        select_all = kwargs.pop("select_all", False)

        filter_result = []
        where_values = False

        if not select_all:
            where = self._process_filters(**kwargs)
            query = self._build_query(where=where, limit=result_limit, order_by=order_by)
            where_values = list(kwargs.values())

        else:
            query = self._build_query()

        try:
            self._db_query(query, where_values)

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
