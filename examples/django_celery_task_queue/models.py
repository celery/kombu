from __future__ import with_statement
from django.db import models
from django.conf import settings
from django.contrib.auth.models import User
from kombu import Connection
import datetime
import uuid

class SampleModel(models.Model):
  """Just a sample Django model."""
  user = models.ForeignKey(User)
  data = models.TextField()

  @classmethod
  def create_task(cls, data):
    """Class method to create a record and associated task."""
    record = cls(**data)
    record.save()

    with Connection(settings.BROKER_URL) as conn:
      queue = conn.SimpleQueue('celery')
      message = {
        'task': 'process-next-task',
        'id': str(uuid.uuid4()),
        'args': [record.id],
        "kwargs": {},
        "retries": 0,
        "eta": str(datetime.datetime.now())  
      }
      queue.put(message)
      queue.close()

    return record

