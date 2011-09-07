from kombu import entity
from kombu.utils import gen_unique_id as uuid


def entry_to_queue(queue, **options):
    binding_key = options.get("binding_key") or options.get("routing_key")

    e_durable = options.get("exchange_durable")
    if e_durable is None:
        e_durable = options.get("durable")

    e_auto_delete = options.get("exchange_auto_delete")
    if e_auto_delete is None:
        e_auto_delete = options.get("auto_delete")

    q_durable = options.get("queue_durable")
    if q_durable is None:
        q_durable = options.get("durable")

    q_auto_delete = options.get("queue_auto_delete")
    if q_auto_delete is None:
        q_auto_delete = options.get("auto_delete")

    e_arguments = options.get("exchange_arguments")
    q_arguments = options.get("queue_arguments")
    b_arguments = options.get("binding_arguments")

    exchange = entity.Exchange(options.get("exchange"),
                               type=options.get("exchange_type"),
                               delivery_mode=options.get("delivery_mode"),
                               routing_key=options.get("routing_key"),
                               durable=e_durable,
                               auto_delete=e_auto_delete,
                               arguments=e_arguments)

    return entity.Queue(queue,
                        exchange=exchange,
                        routing_key=binding_key,
                        durable=q_durable,
                        exclusive=options.get("exclusive"),
                        auto_delete=q_auto_delete,
                        no_ack=options.get("no_ack"),
                        queue_arguments=q_arguments,
                        binding_arguments=b_arguments)
