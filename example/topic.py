from kombu.connection import BrokerConnection
from kombu.messaging import Exchange, Queue, Consumer, Producer

# configuration, normally in an ini file
exchange_name = "test.shane"
exchange_type = "topic"
exchange_durable = True
message_serializer = "json"
queue_name = "test.q"

# 1. setup the connection to the exchange
# hostname,userid,password,virtual_host not used with memory backend
cons_conn = BrokerConnection(hostname="localhost",
                              userid="guest",
                              password="guest",
                              virtual_host="/",
                              transport="memory")
cons_chan = cons_conn.channel()
cons_exch = Exchange(exchange_name, type=exchange_type, durable=exchange_durable)

pub_conn = BrokerConnection(hostname="localhost",
                              userid="guest",
                              password="guest",
                              virtual_host="/",
                              transport="memory")
pub_chan = pub_conn.channel()
pub_exch = Exchange(exchange_name, type=exchange_type, durable=exchange_durable)

# 2. setup the consumer, the consumer declares/creates the queue, if you
#    publish to a queue before there is a consumer it will fail unless the queue
#    was first created and is durable
class AConsumer:
    def __init__(self, queue_name, key):
        self.queue = Queue(queue_name, exchange=cons_exch, routing_key=key)
        self.consumer = Consumer(cons_chan, [self.queue])
        self.consumer.consume()

        def mq_callback(message_data, message):
            print("%s: %r: %r" % (key, message.delivery_info, message_data,))
            #message.ack()
        self.consumer.register_callback(mq_callback)

c1 = AConsumer("test_1","test.1")
c2 = AConsumer("testing","test.ing")
# consumers can use simple pattern matching when defining a queue
c3 = AConsumer("test_all","test.*")

# 3. publish something to consume
# publishers always send to a specific route, the mq will route to the queues
producer = Producer(pub_chan, exchange=pub_exch, serializer=message_serializer)
producer.publish({"name": "Shane Caraveo", "username": "mixedpuppy"}, routing_key="test.1")
producer.publish({"name": "Micky Mouse", "username": "donaldduck"}, routing_key="test.ing")
producer.publish({"name": "Anonymous", "username": "whoami"}, routing_key="test.foobar")

def have_messages():
    return sum([q.qsize() for q in cons_chan.queues.values()])

# 5. run the event loop
while have_messages():
    try:
        cons_conn.drain_events()
    except KeyboardInterrupt:
        print
        print "quitting"
        break
    except Exception, e:
        import traceback
        print traceback.format_exc()
        break

