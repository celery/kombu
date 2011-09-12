What is messaging?
==================

In times long ago people didn't have email.
They had the postal service, which would couragely deliver mail,
from hand to hand all over the globe.  Soldiers deployed at wars far away could only
communicate with their families through the postal service, and
posting a letter would mean that the recipient wouldn't actually
receive the letter until weeks or months, and sometimes years later.
It's hard to imagine this today when people are expected to be available
for phone calls every minute of the day.

So humans need to communicate with each other, this shouldn't
be news to anyone, but why would applications want to send
messages?

One example is the banks.
When you transfer money from one bank to another, this is sent
as a message to the banks messaging central. Banks
need to send and receive millions and millions of these
messages every day, and losing a single message would mean either losing
your money (bad) or the banks money (very bad)

Another is the stock exchange, which also have a need
for very high message througputs and strict reliability requirements.

Email is a great way for people to communicate.  It is much faster
than using the postal service, but still using email as a means for
programs to communicate would be like the solider above, waiting
for signs of life from his girlfriend back home.

Messaging Scenarios
===================

* Request/Reply

  The request/reply pattern works like the postal service example.
  A message is addressed to a single recipient, with a return address
  printed on the back.  The receipient may or may not reply to the
  message by sending it back to the original sender.

  Request-Reply is achieved using *direct* exchanges.

* Broadcast

  Broadcast is achieved using *fanout* exchanges.

* Publish/Subscribe

  Pubsub is achieved using *topic* exchanges.

Reliability
===========

For some applications reliability is very important.  Losing a message is
a critical situtation that must never happen.  For other applications
losing a message is fine, it can maybe recover in other ways,
or the message is resent anyway as periodic updates.

AMQP comes with two persistency modes:

* persistent

    Messages are written to disk and survives a broker restart.

* transient

    Messages may or may not be written to disk, as the broker sees fit
    to optimize memory contents.  The messages will not survive a broker
    restart.

Transient messaging is by far the fastest way to send and receive messages,
so persistency comes with a price, but a necessary cost.


