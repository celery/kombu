

.. code-block:: python

    from carrot import Connection, Exchange, Binding
    from carrot import Consumer, Producer

    media_exchange = Exchange("media", "direct", durable=True)
    video_binding = Binding("video", exchange=media_exchange, key="video")

    # connections/channels
    connection = Connection("localhost", "guest", "guest", "/")
    channel = connection.channel()

    # produce
    producer = Producer(channel, exchange=media_exchange, serializer="json")
    producer.publish({"name": "/tmp/lolcat1.avi", "size": 1301013})

    # consume
    consumer = Consumer(channel, video_binding)
    consumer.register_callback(process_media)
    consumer.consume()

    while True:
        connection.drain_events()


    # consumerset:
    video_binding = Binding("video", exchange=media_exchange, key="video")
    image_binding = Binding("image", exchange=media_exchange, key="image")

    consumer = Consumer(channel, [video_binding, image_binding])









