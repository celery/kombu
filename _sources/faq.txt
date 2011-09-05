============================
 Frequently Asked Questions
============================

Questions
=========

Q: Message.reject doesn't work?
--------------------------------------
**Answer**: RabbitMQ (as of v1.5.5) has not implemented reject yet.
There was a brief discussion about it on their mailing list, and the reason
why it's not implemented yet is revealed:

http://lists.rabbitmq.com/pipermail/rabbitmq-discuss/2009-January/003183.html

Q: Message.requeue doesn't work?
--------------------------------------

**Answer**: See _`Message.reject doesn't work?`
