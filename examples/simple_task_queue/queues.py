from kombu import Exchange, Queue

task_exchange = Exchange("tasks", type="direct")
task_queues = [Queue("hipri", task_exchange, routing_key="hipri"),
               Queue("midpri", task_exchange, routing_key="midpri"),
               Queue("lopri", task_exchange, routing_key="lopri")]
