from app.models import SampleModel
from celery import task
import json

@task(serializer='json', name='process-next-task')
def process_next_task(model_id):
  """Process next task in queue """
  try:
    task = SampleModel.objects.get(id=int(model_id))
  except SampleModel.DoesNotExist:
    pass
  else:
    # Do stuff here
    pass

