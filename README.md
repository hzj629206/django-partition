# MySQL Partition for Django 1.11+
Usage:

* patch in {app}/app.py
```python
from partition.db import patch_for_partition
patch_for_partition()
```

* settings.py
```python
DATABASE_ROUTERS = ['partition.db.DatabaseRouter']

DATABASES = {
	"db1.write": {},
	"db1.read.0": {},
	"db1.read.1": {}
}
```

* models.py
```python
from partition.db import PartitionManager, partition_by_thousand

class MyModel(models.Model):
	objects = PartitionManager()
	
	user_id = models.BigIntegerField()
	user_name = models.CharField()
	create_time = models.DateTimeField(auto_add=True)

	class Meta:
		db_table = "my_tbl_%s"
		db_name = "my_db_%s"
		db_conn = "db1"
		partition_func = partition_by_thousand()
		partition_field = "user_id"  # this is optional
```

* migrate
```bash
python manage.py makemigrations
python manage.py sqlmigrate
python manage.py migrate
```

* usage
```python
user = MyModel.objects.create(user_id=100, user_name="user1")
user.user_name = "user2"
user.save()

MyModel.objects.filter(uid=100000)

MyModel.objects.partition(pid=0).count()
MyModel.objects.partition(100).count()
```
