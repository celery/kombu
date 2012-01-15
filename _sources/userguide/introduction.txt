.. _guide-intro:

==============
 Introduction
==============

.. _intro-messaging:

What is messaging?
==================

In times long ago people didn't have email.
They had the postal service, which with great courage would deliver mail
from hand to hand all over the globe.  Soldiers deployed at wars far away could only
communicate with their families through the postal service, and
posting a letter would mean that the recipient wouldn't actually
receive the letter until weeks or months, sometimes years later.

It's hard to imagine this today when people are expected to be available
for phone calls every minute of the day.

So humans need to communicate with each other, this shouldn't
be news to anyone, but why would applications?

One example is banks.
When you transfer money from one bank to another, your bank sends
a message to the banks messaging central.  The messaging central
then record and coordinate the transaction.  Banks
need to send and receive millions and millions of
messages every day, and losing a single message would mean either losing
your money (bad) or the banks money (very bad)

Another example is the stock exchanges, which also have a need
for very high message throughputs and have strict reliability
requirements.

Email is a great way for people to communicate.  It is much faster
than using the postal service, but still using email as a means for
programs to communicate would be like the soldier above, waiting
for signs of life from his girlfriend back home.

.. _messaging-scenarios:

Messaging Scenarios
===================

* Request/Reply

  The request/reply pattern works like the postal service example.
  A message is addressed to a single recipient, with a return address
  printed on the back.  The recipient may or may not reply to the
  message by sending it back to the original sender.

  Request-Reply is achieved using *direct* exchanges.

* Broadcast

  In a broadcast scenario a message is sent to all parties.
  This could be none, one or many recipients.

  Broadcast is achieved using *fanout* exchanges.

* Publish/Subscribe

  In a publish/subscribe scenario producers publish messages
  to topics, and consumers subscribe to the topics they are
  interested in.

  If no consumers subscribe to the topic, then the message
  will not be delivered to anyone.  If several consumers
  subscribe to the topic, then the message will be delivered
  to all of them.

  Pub-sub is achieved using *topic* exchanges.

.. _messaging-reliability:

Reliability
===========

For some applications reliability is very important.  Losing a message is
a critical situation that must never happen.  For other applications
losing a message is fine, it can maybe recover in other ways,
or the message is resent anyway as periodic updates.

AMQP defines two built-in delivery modes:

* persistent

    Messages are written to disk and survives a broker restart.

* transient

    Messages may or may not be written to disk, as the broker sees fit
    to optimize memory contents.  The messages will not survive a broker
    restart.

Transient messaging is by far the fastest way to send and receive messages,
so having persistent messages comes with a price, but for some
applications this is a necessary cost.
