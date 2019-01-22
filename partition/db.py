# -*- coding: utf-8 -*-

import random
import socket
import logging
from contextlib import contextmanager
from functools import partial
from zlib import crc32
from datetime import datetime, date, timedelta
from threading import local
from django.db import router, connections, DEFAULT_DB_ALIAS
from django.db.models import Manager, QuerySet
from django.utils.six import binary_type
from django.core.cache import cache

log = logging.getLogger("main")


class Routers(object):
    def __getattr__(self, name):
        for r in router.routers:
            if hasattr(r, name):
                return getattr(r, name)
        raise AttributeError('Not found the router with the attribute "%s".' % name)


routers = Routers()
"""
Usage:
# reset for each request
routers.init()
routers.init('master')
routers.init('slave')

with routers.using('master'):
    pass

with routers.using('slave'):
    pass

@atomic(using=routers.db_for_write(model))
or
with atomic(using=routers.db_for_write(model)):
"""


hostname = socket.getfqdn()


def get_object_name(obj):
    try:
        return obj.__name__
    except AttributeError:
        return obj.__class__.__name__


def is_alive(connection):
    if connection.connection is not None and hasattr(connection.connection, 'ping'):
        log.debug('Ping db: %s', connection.alias)
        try:
            # Since MySQL-python 1.2.2 connection.ping()
            # takes an optional boolean argument to enable automatic reconnection.
            # https://github.com/farcepest/MySQLdb1/blob/d34fac681487541e4be07e6978e0db233faf8252/HISTORY#L103
            connection.connection.ping(True)
        except TypeError:
            connection.connection.ping()
    else:
        log.debug('Get cursor for db: %s', connection.alias)
        with connection.cursor():
            pass
    return True


def is_writable(connection):
    with connection.cursor() as cursor:
        cursor.execute('SELECT @@read_only')
        result = not int(cursor.fetchone()[0])
    return result


def check_db(checker, db_name, cache_seconds=None, number_of_tries=1, force=False):
    assert number_of_tries >= 1, 'Number of tries must be >= 1.'

    connection = connections[db_name]

    checker_name = get_object_name(checker)
    cache_key = ':'.join((hostname, checker_name, db_name))
    dead_mark = 'dead'

    if not force and cache_seconds is not None:
        is_dead = cache.get(cache_key) == dead_mark

        if is_dead:
            log.debug(
                'Check "%s" %s was failed less than %d ago, no check needed',
                checker_name, db_name, cache_seconds
            )

            return False
        else:
            log.debug(
                'Last check "%s" %s succeeded or was more than %d ago, checking again',
                db_name, checker_name, cache_seconds
            )
    else:
        log.debug('Force check %s: %s', checker_name, db_name)

    result = False
    for count in range(1, number_of_tries + 1):
        log.debug(
            'Trying to check "%s" %s: %d try',
            db_name, checker_name, count
        )

        try:
            result = checker(connection)
        except Exception as ex:
            if count == number_of_tries:
                log.exception('Error verifying %s: %s, %s', checker_name, db_name, ex)

            result = False

        log.debug(
            'After %d tries "%s" %s = %s',
            count, db_name, checker_name, result
        )

        if result:
            break

    if not result and cache_seconds is not None:
        cache.set(cache_key, dead_mark, cache_seconds)

    return result


db_is_alive = partial(check_db, is_alive)
db_is_writable = partial(check_db, is_writable)


class DatabaseRouter(object):
    def __init__(self):
        from django.conf import settings

        self._context = local()
        self._context.initialized = False

        self._databases = getattr(settings, 'DATABASES', {})
        self._app_mapping = getattr(settings, 'DATABASE_APPS_MAPPING', {})
        self._downtime = getattr(settings, 'DATABASE_DOWNTIME', 60)

    @property
    def context(self):
        if not getattr(self._context, 'initialized', False):
            self.reset()
        return self._context

    def reset(self):
        self._context.initialized = True
        self._context.read_selected = {}
        self._context.write_selected = {}
        self._context.state_stack = []

    def init(self, state=None):
        self.reset()
        if state is not None:
            self.enter(state)

    def state(self):
        """
        Current state of routing: 'master' or 'slave'.
        """
        if self.context.state_stack:
            return self.context.state_stack[-1]
        else:
            return None

    def enter(self, state):
        """
        Switches router into a new state. Requires a paired call
        to 'revert' for reverting to previous state.
        """
        assert state in ['master', 'slave']
        self.context.state_stack.append(state)
        return self

    def revert(self):
        """
        Reverts wrapper state to a previous value after calling
        'enter'.
        """
        self.context.state_stack.pop()
        return self

    @contextmanager
    def using(self, state):
        self.enter(state)
        yield self
        self.revert()

    def is_alive(self, name):
        return db_is_alive(name, self._downtime)

    def get_model_connection(self, model):
        db_conn = getattr(model._meta, 'db_conn', None)
        if db_conn:
            return db_conn

        app_label = model._meta.app_label
        db_conn = self._app_mapping.get(app_label)
        if db_conn:
            return db_conn

        return None

    def select_connection(self, name, write):
        prefix = "%s.%s" % (name, "write" if write else "read")
        conns = [k for k in self._databases.keys() if k.startswith(prefix)]

        random.shuffle(conns)
        if len(conns) > 1 or not write:
            for conn in conns:
                if self.is_alive(conn):
                    return conn
        elif len(conns) > 0:
            return conns[0]

        if not write:
            return self.select_connection(name, True)

        log.fatal("all of the database connections are not available for [db_conn:%s], [writable:%s]", name, write)
        raise RuntimeError("All of the database connections are not available.")

    def db_for_read(self, model, **hints):
        if self.state() == 'master':
            return self.db_for_write(model, **hints)

        db_conn = self.get_model_connection(model)
        if db_conn:
            if db_conn not in self.context.read_selected:
                self.context.read_selected[db_conn] = self.select_connection(db_conn, False)
                log.debug(
                    'db_for_read: %s, [db_conn:%s], [model:%s]',
                    self.context.read_selected[db_conn],
                    db_conn, model._meta.model_name
                )
            return self.context.read_selected[db_conn]

        return None

    def db_for_write(self, model, **hints):
        if self.state() == 'slave':
            raise RuntimeError('Trying to access master database in slave state')

        db_conn = self.get_model_connection(model)
        if db_conn:
            if db_conn not in self.context.write_selected:
                self.context.write_selected[db_conn] = self.select_connection(db_conn, True)
                log.debug(
                    'db_for_write: %s, [db_conn:%s], [model:%s]',
                    self.context.write_selected[db_conn],
                    db_conn, model._meta.model_name
                )
            return self.context.write_selected[db_conn]

        return None

    def allow_relation(self, obj1, obj2, **hints):
        if obj1._state.db == obj2._state.db == DEFAULT_DB_ALIAS:
            return None
        if any([
            obj1._state.db or self.db_for_write(obj1.__class__, **hints),
            obj2._state.db or self.db_for_write(obj2.__class__, **hints),
        ]):
            return False
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if model_name:
            from django.apps import apps

            model = apps.get_model(app_label, model_name)
            db_conn = self.get_model_connection(model)
            if db_conn:
                return db.startswith("%s.write" % db_conn)
            elif db != DEFAULT_DB_ALIAS:
                return False

        return None


def monkeypatch_method(cls, name=None):
    import functools

    def decorator(func):
        func_name = name or func.__name__
        old_func = getattr(cls, name, None)
        if old_func:
            func = functools.wraps(old_func)(func)
        setattr(cls, func_name, func)
        setattr(func, "old_func", old_func)
        return func
    return decorator


def patch_for_partition():
    """
    patch db_name for MySQL
    Usage:
    class Meta:
        db_name = "db_partition_00"
        db_table = "t_partition_0"
    """
    from django.db.models.sql.datastructures import BaseTable, Join
    from django.db.models import options
    from django.db.models.sql import Query
    from django.db.models.sql.compiler import SQLInsertCompiler, SQLUpdateCompiler, SQLDeleteCompiler
    from django.db.models.sql.subqueries import DeleteQuery, UpdateQuery
    from django.db.models.expressions import Col
    from django.db.models.query import ModelIterable
    from django.db.models.base import ModelBase
    from django.db.backends import utils

    # more Meta options
    setattr(options, "DEFAULT_NAMES", options.DEFAULT_NAMES + (
        "db_name", "partition_func", "partition_field", "db_conn",
    ))

    @monkeypatch_method(ModelBase, "_do_update")
    def model_base_do_update(self, base_qs, using, pk_val, values, update_fields, forced_update):
        """
        copy partition values from model instance to query instance
        """
        base_qs.query.partition_id = getattr(self, "partition_id", None)
        base_qs.query.partition_key = getattr(self, "partition_key", None)
        return model_base_do_update.old_func(self, base_qs, using, pk_val, values, update_fields, forced_update)

    @monkeypatch_method(ModelIterable, "__iter__")
    def model_iterable_iter(self):
        """
        copy partition values from query instance to model instance
        """
        query = self.queryset.query
        partition_id = getattr(query, "partition_id", None)
        partition_key = getattr(query, "partition_key", None)
        for obj in model_iterable_iter.old_func(self):
            if partition_id is not None:
                setattr(obj, "partition_id", partition_id)
            if partition_key is not None:
                setattr(obj, "partition_key", partition_key)
            yield obj

    @monkeypatch_method(utils, "split_identifier")
    def split_identifier(identifier):
        """
        support mysql full identifier style `db_name`.`table_name`
        """
        if "`" not in identifier:
            return split_identifier.old_func(identifier)

        try:
            namespace, name = identifier.split('`.`')
        except ValueError:
            namespace, name = '', identifier
        return namespace.strip('`'), name.strip('`')

    @monkeypatch_method(UpdateQuery, "add_update_fields")
    def update_query_add_update_fields(self, values_seq):
        """
        automatically set partition_key by partition_field for UPDATE
        """
        update_query_add_update_fields.old_func(self, values_seq)

        opts = self.get_meta()
        partition_field = getattr(opts, "partition_field", None)
        if partition_field:
            for field, _, val in self.values:
                if field.name == partition_field:
                    self.partition_key = val

    @monkeypatch_method(DeleteQuery, "delete_qs")
    def delete_query_delete_qs(self, query, using):
        """
        copy partition values from origin query for DELETE
        """
        self.partition_key = getattr(query.query, "partition_key", None)
        self.partition_id = getattr(query.query, "partition_id", None)

        return delete_query_delete_qs.old_func(self, query, using)

    @monkeypatch_method(SQLInsertCompiler, "as_sql")
    def insert_compiler_as_sql(self):
        # set partition values for INSERT
        opts = self.query.get_meta()
        partition_field = getattr(opts, "partition_field", None)
        partition_key = None
        partition_id = None
        for obj in self.query.objs:
            tmp_key = getattr(obj, "partition_key", None)
            if partition_field:
                tmp_key = getattr(obj, partition_field, tmp_key)  # obj.${partition_field} may be not set
            if partition_key is not None and partition_key != tmp_key:
                raise RuntimeError("Multiple partitions were found in INSERT operation")
            partition_key = tmp_key
            tmp_id = getattr(obj, "partition_id", None)
            if partition_id is not None and partition_id != tmp_id:
                raise RuntimeError("Multiple partitions were found in INSERT operation")
            partition_id = tmp_id
        if partition_key is not None:
            self.query.partition_key = partition_key
        if partition_id is not None:
            self.query.partition_id = partition_id

        # get full table name (with db name and partition suffix)
        self.query.get_initial_alias()
        qn = self.connection.ops.quote_name
        opts = self.query.get_meta()
        table = opts.db_table
        full_table = self.query.alias_map[table].as_sql(self, self.connection)[0]

        r = insert_compiler_as_sql.old_func(self)

        # replace first one
        return [(_sql.replace(qn(table), full_table, 1), _params) for _sql, _params in r]

    @monkeypatch_method(SQLUpdateCompiler, "as_sql")
    def update_compiler_as_sql(self):
        # get full table name (with db name and partition suffix)
        self.query.get_initial_alias()
        qn = self.quote_name_unless_alias
        table = self.query.tables[0]
        full_table = self.query.alias_map[table].as_sql(self, self.connection)[0]

        sql, params = update_compiler_as_sql.old_func(self)

        # replace first one
        return sql.replace(qn(table), full_table, 1), params

    @monkeypatch_method(SQLDeleteCompiler, "as_sql")
    def delete_compiler_as_sql(self):
        # get full table name (with db name and partition suffix)
        qn = self.quote_name_unless_alias
        table = self.query.tables[0]
        full_table = self.query.alias_map[table].as_sql(self, self.connection)[0]

        sql, params = delete_compiler_as_sql.old_func(self)

        # replace first one
        return sql.replace(qn(table), full_table, 1), params

    @monkeypatch_method(BaseTable, "as_sql")
    def base_table_as_sql(self, compiler, connection):
        if not self.db_name:
            return base_table_as_sql.old_func(self, compiler, connection)
        else:
            # append db_name, db_name_suffix, table_name_suffix
            table_name, db_name = self.table_name, self.db_name
            db_name_suffix = getattr(self, "db_name_suffix", None)
            table_name_suffix = getattr(self, "table_name_suffix", None)
            if db_name_suffix is not None:
                db_name = db_name % db_name_suffix
            if table_name_suffix is not None:
                table_name = table_name % table_name_suffix
            alias_str = '' if self.table_alias == self.table_name else (' %s' % self.table_alias)
            base_sql = "%s.%s" % (compiler.quote_name_unless_alias(db_name), compiler.quote_name_unless_alias(table_name))
            return base_sql + alias_str, []

    @monkeypatch_method(BaseTable, "relabeled_clone")
    def base_table_relabeled_clone(self, change_map):
        r = base_table_relabeled_clone.old_func(self, change_map)

        # copy extra data
        r.db_name = self.db_name
        r.db_name_suffix = getattr(self, "db_name_suffix", None)
        r.table_name_suffix = getattr(self, "table_name_suffix", None)
        return r

    @monkeypatch_method(Col, "as_sql")
    def col_as_sql(self, compiler, connection):
        bt_or_join = compiler.query.alias_map[self.alias]
        if bt_or_join.table_name != self.alias:
            qn = compiler.quote_name_unless_alias
            return "%s.%s" % (qn(self.alias), qn(self.target.column)), []
        else:
            # append table name suffix to first table(Meta.db_table)
            table_name_suffix = getattr(bt_or_join, "table_name_suffix", None)
            if table_name_suffix is not None:
                alias = self.alias % table_name_suffix
            else:
                alias = self.alias
            qn = compiler.quote_name_unless_alias
            return "%s.%s" % (qn(alias), qn(self.target.column)), []

    @monkeypatch_method(Join, "as_sql")
    def join_as_sql(self, compiler, connection):
        if not self.db_name:
            return join_as_sql.old_func(self, compiler, connection)
        else:
            sql, params = join_as_sql.old_func(self, compiler, connection)

            # append db name, db name suffix, table name suffix
            db_name, table_name = self.db_name, self.table_name
            table_alias, parent_alias = self.table_alias, self.parent_alias

            db_name_suffix = getattr(self, "db_name_suffix", None)
            table_name_suffix = getattr(self, "table_name_suffix", None)
            if db_name_suffix is not None:
                db_name = db_name % db_name_suffix
            if table_name_suffix is not None:
                if table_alias == table_name:
                    table_alias = table_alias % table_name_suffix
                table_name = table_name % table_name_suffix

            pbt = compiler.query.alias_map[parent_alias]
            parent_table_name_suffix = getattr(pbt, "table_name_suffix", None)
            if parent_alias == pbt.table_name and parent_table_name_suffix is not None:
                parent_alias = parent_alias % parent_table_name_suffix

            qn = compiler.quote_name_unless_alias
            full_table = "%s.%s" % (qn(db_name), qn(table_name))
            parent_alias = qn(parent_alias)
            table_alias = qn(table_alias)

            # replace first one
            sql = sql.replace(qn(self.table_name), full_table, 1)
            if self.parent_alias != parent_alias:
                sql = sql.replace(qn(self.parent_alias), qn(parent_alias))
            if self.table_alias != table_alias:
                sql = sql.replace(qn(self.table_alias), qn(table_alias))
            return sql, params

    @monkeypatch_method(Join, "relabeled_clone")
    def join_relabeled_clone(self, change_map):
        r = join_relabeled_clone.old_func(self, change_map)

        # copy extra data
        r.db_name = self.db_name
        r.db_name_suffix = getattr(self, "db_name_suffix", None)
        r.table_name_suffix = getattr(self, "table_name_suffix", None)
        return r

    @monkeypatch_method(Query, "clone")
    def query_clone(self, klass=None, memo=None, **kwargs):
        obj = query_clone.old_func(self, klass, memo, **kwargs)

        # copy extra data
        obj.partition_key = getattr(self, "partition_key", None)
        obj.partition_id = getattr(self, "partition_id", None)
        return obj

    @monkeypatch_method(Query, "build_filter")
    def query_build_filter(
        self, filter_expr, branch_negated=False, current_negated=False,
        can_reuse=None, connector='AND', allow_joins=True, split_subq=True
    ):
        # set partition_key for filter
        partition_field = getattr(self.get_meta(), "partition_field", None)
        if partition_field:
            arg, value = filter_expr
            if arg:
                lookups, parts, reffed_expression = self.solve_lookup_type(arg)
                if len(parts) == 1 and parts[0] == partition_field and lookups[0] in ["exact", "iexact"]:
                    if not current_negated:
                        self.partition_key = value
                    elif getattr(self, "partition_key", None) is None:
                        self.partition_key = value
                    elif not branch_negated:
                        self.partition_key = value

        return query_build_filter.old_func(
            self, filter_expr, branch_negated, current_negated,
            can_reuse, connector, allow_joins, split_subq
        )

    @monkeypatch_method(Query, "get_initial_alias")
    def query_get_initial_alias(self):
        if self.tables:
            alias = self.tables[0]
            self.ref_alias(alias)
        else:
            bt = BaseTable(self.get_meta().db_table, None)
            bt.db_name = getattr(self.get_meta(), "db_name", None)
            bt.db_name_suffix = None
            bt.table_name_suffix = None
            alias = self.join(bt)

        # update partition information
        opts = self.get_meta()

        partition_key = getattr(self, "partition_key", None)
        partition_id = getattr(self, "partition_id", None)
        partition_func = getattr(opts, "partition_func", None)
        # ignore partition_key if partition_id is set
        if partition_id is None and partition_key is not None and partition_func:
            partition_func = opts.partition_func
            if hasattr(partition_func, "__func__"):
                partition_func = opts.partition_func.__func__
            partition_id = partition_func(partition_key)

        # ignore for normal Model, partition_func is required for partition Model
        if partition_id is not None and not partition_func:
            partition_id = None

        db_name_suffix = None
        table_name_suffix = None
        if partition_id is not None:
            if isinstance(partition_id, tuple) and len(partition_id) > 1:
                db_name_suffix = partition_id[0]
                table_name_suffix = partition_id[1]
            else:
                table_name_suffix = partition_id

        bt = self.alias_map[alias]
        if db_name_suffix is not None:
            bt.db_name_suffix = db_name_suffix
        if table_name_suffix is not None:
            bt.table_name_suffix = table_name_suffix

        return alias

    @monkeypatch_method(Query, "trim_start")
    def query_trim_start(self, names_with_path):
        # make a copy of alias_map
        c = self.alias_map.copy()

        r = query_trim_start.old_func(self, names_with_path)

        for table in self.tables:
            if self.alias_refcount[table] > 0:
                bt = self.alias_map[table]

                # copy extra data
                bt.db_name = c[table].db_name
                bt.db_name_suffix = getattr(c[table], "db_name_suffix", None)
                bt.table_name_suffix = getattr(c[table], "table_name_suffix", None)

                break

        return r

    @monkeypatch_method(Query, "setup_joins")
    def query_setup_join(self, names, opts, alias, can_reuse=None, allow_many=True):
        final_field, targets, opts, joins, path = query_setup_join.old_func(
            self, names, opts, alias, can_reuse, allow_many
        )

        # update partition information
        partition_key = getattr(self, "partition_key", None)
        partition_id = getattr(self, "partition_id", None)
        partition_func = getattr(opts, "partition_func", None)
        # ignore partition_key if partition_id is set
        if partition_id is None and partition_key is not None and partition_func:
            partition_func = opts.partition_func
            if hasattr(partition_func, "__func__"):
                partition_func = opts.partition_func.__func__
            partition_id = partition_func(partition_key)

        # ignore for normal Model, partition_func is required for partition Model
        if partition_id is not None and not partition_func:
            partition_id = None

        db_name_suffix = None
        table_name_suffix = None
        if partition_id is not None:
            if isinstance(partition_id, tuple) and len(partition_id) > 1:
                db_name_suffix = partition_id[0]
                table_name_suffix = partition_id[1]
            else:
                table_name_suffix = partition_id

        for _alias in joins[1:]:
            join = self.alias_map[_alias]
            join.db_name = getattr(opts, "db_name", None)
            join.db_name_suffix = db_name_suffix
            join.table_name_suffix = table_name_suffix

        return final_field, targets, opts, joins, path


class PartitionQuerySet(QuerySet):
    def partition(self, pkey=None, pid=None):
        """
        set partition_key or partition_id for Query explicitly
        this can be overridden if Meta.partition_field is set
        this will be ignored if partition_id is set
        :param pkey: parameter for Meta.partition_func
        :param pid: pid = pid or Meta.partition_func(pkey)
        :rtype: PartitionQuerySet
        """
        if pid is not None:
            self.query.partition_id = pid
        elif pkey is not None:
            self.query.partition_key = pkey
        else:
            self.query.partition_id = getattr(self.query, "partition_id", None)
            self.query.partition_key = getattr(self.query, "partition_key", None)
        return self

    def using_db_for_write(self, for_write=True):
        self._for_write = for_write

    def create(self, **kwargs):
        """
        copy partition values from origin query to model instance
        """
        obj = self.model(**kwargs)

        # copy partition values
        partition_id = getattr(self.query, "partition_id", None)
        partition_key = getattr(self.query, "partition_key", None)
        if partition_id is not None:
            setattr(obj, "partition_id", partition_id)
        if partition_key is not None:
            setattr(obj, "partition_key", partition_key)
        # end copy partition values

        self._for_write = True
        obj.save(force_insert=True, using=self.db)
        return obj


class PartitionManager(Manager):
    """
    Usage:

    class UserLoginLogTab(Model):
        objects = PartitionManager()

        id = db.BigAutoField(primary_key=True)
        uid = db.PositiveBigIntegerField()
        login_time = db.PositiveIntegerField()

        class Meta:
            # db_table is required
            db_table = u'user_login_log_tab_%s'

            # db_name is required
            db_name = u'user_db_%s'

            # use helper function to define partition function
            # partition_func is required
            partition_func = partition_by_datetime('%Y%m%d')

            # partition_field is optional
            partition_field = u'uid'

    UserLoginLogTab.objects.partition(pid=0).count()
    UserLoginLogTab.objects.partition(100000).count()
    UserLoginLogTab.objects.filter(uid=100000)
    UserLoginLogTab(uid=100000,login_time=time.time()).save()
    UserLoginLogTab.objects.filter(uid=100000).delete()
    """
    use_for_related_fields = True

    def get_queryset(self):
        """
        :rtype: PartitionQuerySet
        """
        qs = PartitionQuerySet(self.model, using=self._db, hints=self._hints).partition()
        partition_field = getattr(self.model._meta, "partition_field", None)
        if partition_field and "instance" in self._hints:
            instance = self._hints["instance"]
            qs.partition(getattr(instance, partition_field, None))
        return qs

    def partition(self, pkey=None, pid=None):
        """
        set partition_key or partition_id for Query explicitly
        this can be overridden if Meta.partition_field is set
        this will be ignored if partition_id is set
        :param pkey: parameter for Meta.partition_func
            the result of partition_func(partition_key) can be a single value for table name,
            or a tuple for both db name and table name
        :param pid: pid = pid or Meta.partition_func(pkey)
        :rtype: PartitionQuerySet
        """
        return self.get_queryset().partition(pkey, pid)

    def using_db_for_write(self, for_write=True):
        """
        :param bool for_write:
        :rtype: PartitionQuerySet
        """
        return self.get_queryset().using_db_for_write(for_write)

    def db_manager_for_write(self, using=None, hints=None):
        return self.db_manager(using or router.db_for_write(self.model, **self._hints), hints)


def partition_by_mod(base, crc=False):
    def func(n):
        if crc:
            n = crc32(binary_type(n))
        return n % base

    def func_iter():
        for i in range(base):
            yield i

    func.iter = func_iter
    return func


def partition_by_div(base, crc=False, max_num=None):
    def func(n):
        if crc:
            n = crc32(binary_type(n))
        return n // base

    def func_iter():
        from django.conf import settings

        m = max_num or getattr(settings, 'PARTITION_MAX_NUMBER', 1)
        for i in range(m):
            yield i

    func.iter = func_iter
    return func


def partition_by_datetime(fmt, start_date=None, end_date=None):
    def func(timestamp):
        if isinstance(timestamp, (datetime, date)):
            return timestamp.strftime(fmt)
        else:
            return datetime.fromtimestamp(int(timestamp)).strftime(fmt)

    def func_iter():
        from django.conf import settings

        sd = start_date or getattr(settings, 'PARTITION_START_DATE', datetime.now().date())
        ed = end_date or getattr(settings, 'PARTITION_END_DATE', datetime.now().date())

        while sd <= ed:
            yield sd.strftime('%Y%m%d')
            sd += timedelta(days=1)

    func.iter = func_iter
    return func


def partition_by_thousand(crc=False):
    def func(n):
        if crc:
            n = crc32(binary_type(n))
        n = binary_type(n)
        n = n.zfill(3)
        return n[-3:-1], n[-1]

    def func_iter():
        for i in range(1000):
            db = i // 10
            tbl = i % 10
            yield "%02d" % db, tbl

    func.iter = func_iter
    return func
