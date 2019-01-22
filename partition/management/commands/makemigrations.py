from django.core.management.commands import makemigrations


class Command(makemigrations.Command):
    def handle(self, *app_labels, **options):
        from django.apps import apps

        # create fake models and connections
        models = apps.get_models()
        if app_labels:
            models = [m for m in models if m._meta.app_label in app_labels]

        db = makemigrations.settings.DATABASES.keys()[0]
        connection = makemigrations.connections[db]
        qn = connection.ops.quote_name

        partition_models = []
        partition_databases = []

        for model in models:
            if hasattr(model._meta, "partition_func"):
                model._meta.abstract = True
                partition_func = getattr(model._meta, "partition_func")
                if hasattr(partition_func, '__func__'):
                    partition_func = partition_func.__func__

                # create and register fake partition models
                for partition_id in partition_func.iter():
                    if isinstance(partition_id, tuple):
                        _db, _tbl = partition_id
                    else:
                        _db, _tbl = None, partition_id
                    meta = {
                        'default_permissions': tuple(),
                    }

                    db_conn = getattr(model._meta, "db_conn", None)
                    if db_conn:
                        meta['db_conn'] = db_conn

                    indexes = getattr(model._meta, "indexes", None)
                    if indexes:
                        meta['indexes'] = indexes

                    index_together = getattr(model._meta, "index_together", None)
                    if index_together:
                        meta['index_together'] = index_together

                    unique_together = getattr(model._meta, "unique_together", None)
                    if unique_together:
                        meta['unique_together'] = unique_together

                    model_name = model.__name__

                    db_name = getattr(model._meta, "db_name", None)
                    if _db is not None and db_name is not None:
                        db_name = db_name % _db
                        model_name = "%s_%s" % (model_name, _db)

                    db_table = getattr(model._meta, "db_table")
                    db_table = db_table % _tbl
                    model_name = "%s_%s" % (model_name, _tbl)

                    if db_name is None:
                        full_name = db_name
                    else:
                        full_name = "%s.%s" % (qn(db_name), qn(db_table))

                    meta['db_table'] = full_name

                    if db_name is not None and db_name not in partition_databases:
                        partition_databases.append(db_name)

                    partition_model = type(str(model_name), (model,), {
                        '__module__': model.__module__,
                        'Meta': type('Meta', tuple(), meta)
                    })
                    partition_models.append(partition_model)

                del apps.all_models[model._meta.app_label][model._meta.model_name]
            elif hasattr(model._meta, "db_name"):
                db_name = getattr(model._meta, "db_name")
                model._meta.db_table = "%s.%s" % (qn(db_name), qn(model._meta.db_table))
                model._meta.original_attrs['db_table'] = model._meta.db_table

        return super(Command, self).handle(*app_labels, **options)
