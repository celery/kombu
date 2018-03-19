# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock

from kombu.asynchronous.aws.sqs.message import AsyncMessage
from kombu.asynchronous.aws.sqs.queue import AsyncQueue

from t.mocks import PromiseMock

from ..case import AWSCase


class test_AsyncQueue(AWSCase):

    def setup(self):
        self.conn = Mock(name='connection')
        self.x = AsyncQueue(self.conn, '/url')
        self.callback = PromiseMock(name='callback')

    def test_message_class(self):
        assert issubclass(self.x.message_class, AsyncMessage)

    def test_get_attributes(self):
        self.x.get_attributes(attributes='QueueSize', callback=self.callback)
        self.x.connection.get_queue_attributes.assert_called_with(
            self.x, 'QueueSize', self.callback,
        )

    def test_set_attribute(self):
        self.x.set_attribute('key', 'value', callback=self.callback)
        self.x.connection.set_queue_attribute.assert_called_with(
            self.x, 'key', 'value', self.callback,
        )

    def test_get_timeout(self):
        self.x.get_timeout(callback=self.callback)
        self.x.connection.get_queue_attributes.assert_called()
        on_ready = self.x.connection.get_queue_attributes.call_args[0][2]
        self.x.connection.get_queue_attributes.assert_called_with(
            self.x, 'VisibilityTimeout', on_ready,
        )

        on_ready({'VisibilityTimeout': '303'})
        self.callback.assert_called_with(303)

    def test_set_timeout(self):
        self.x.set_timeout(808, callback=self.callback)
        self.x.connection.set_queue_attribute.assert_called()
        on_ready = self.x.connection.set_queue_attribute.call_args[0][3]
        self.x.connection.set_queue_attribute.assert_called_with(
            self.x, 'VisibilityTimeout', 808, on_ready,
        )
        on_ready(808)
        self.callback.assert_called_with(808)
        assert self.x.visibility_timeout == 808

        on_ready(None)
        assert self.x.visibility_timeout == 808

    def test_add_permission(self):
        self.x.add_permission(
            'label', 'accid', 'action', callback=self.callback,
        )
        self.x.connection.add_permission.assert_called_with(
            self.x, 'label', 'accid', 'action', self.callback,
        )

    def test_remove_permission(self):
        self.x.remove_permission('label', callback=self.callback)
        self.x.connection.remove_permission.assert_called_with(
            self.x, 'label', self.callback,
        )

    def test_read(self):
        self.x.read(visibility_timeout=909, callback=self.callback)
        self.x.connection.receive_message.assert_called()
        on_ready = self.x.connection.receive_message.call_args[1]['callback']
        self.x.connection.receive_message.assert_called_with(
            self.x, number_messages=1, visibility_timeout=909,
            attributes=None, wait_time_seconds=None, callback=on_ready,
        )

        messages = [Mock(name='message1')]
        on_ready(messages)

        self.callback.assert_called_with(messages[0])

    def MockMessage(self, id, md5):
        m = Mock(name='Message-{0}'.format(id))
        m.id = id
        m.md5 = md5
        return m

    def test_write(self):
        message = self.MockMessage('id1', 'digest1')
        self.x.write(message, delay_seconds=303, callback=self.callback)
        self.x.connection.send_message.assert_called()
        on_ready = self.x.connection.send_message.call_args[1]['callback']
        self.x.connection.send_message.assert_called_with(
            self.x, message.get_body_encoded(), 303,
            callback=on_ready,
        )

        new_message = self.MockMessage('id2', 'digest2')
        on_ready(new_message)
        assert message.id == 'id2'
        assert message.md5 == 'digest2'

    def test_write_batch(self):
        messages = [('id1', 'A', 0), ('id2', 'B', 303)]
        self.x.write_batch(messages, callback=self.callback)
        self.x.connection.send_message_batch.assert_called_with(
            self.x, messages, callback=self.callback,
        )

    def test_delete_message(self):
        message = self.MockMessage('id1', 'digest1')
        self.x.delete_message(message, callback=self.callback)
        self.x.connection.delete_message.assert_called_with(
            self.x, message, self.callback,
        )

    def test_delete_message_batch(self):
        messages = [
            self.MockMessage('id1', 'r1'),
            self.MockMessage('id2', 'r2'),
        ]
        self.x.delete_message_batch(messages, callback=self.callback)
        self.x.connection.delete_message_batch.assert_called_with(
            self.x, messages, callback=self.callback,
        )

    def test_change_message_visibility_batch(self):
        messages = [
            (self.MockMessage('id1', 'r1'), 303),
            (self.MockMessage('id2', 'r2'), 909),
        ]
        self.x.change_message_visibility_batch(
            messages, callback=self.callback,
        )
        self.x.connection.change_message_visibility_batch.assert_called_with(
            self.x, messages, callback=self.callback,
        )

    def test_delete(self):
        self.x.delete(callback=self.callback)
        self.x.connection.delete_queue.assert_called_with(
            self.x, callback=self.callback,
        )

    def test_count(self):
        self.x.count(callback=self.callback)
        self.x.connection.get_queue_attributes.assert_called()
        on_ready = self.x.connection.get_queue_attributes.call_args[0][2]
        self.x.connection.get_queue_attributes.assert_called_with(
            self.x, 'ApproximateNumberOfMessages', on_ready,
        )

        on_ready({'ApproximateNumberOfMessages': '909'})
        self.callback.assert_called_with(909)

    def test_interface__count_slow(self):
        with pytest.raises(NotImplementedError):
            self.x.count_slow()

    def test_interface__dump(self):
        with pytest.raises(NotImplementedError):
            self.x.dump()

    def test_interface__save_to_file(self):
        with pytest.raises(NotImplementedError):
            self.x.save_to_file()

    def test_interface__save_to_filename(self):
        with pytest.raises(NotImplementedError):
            self.x.save_to_filename()

    def test_interface__save(self):
        with pytest.raises(NotImplementedError):
            self.x.save()

    def test_interface__save_to_s3(self):
        with pytest.raises(NotImplementedError):
            self.x.save_to_s3()

    def test_interface__load_from_s3(self):
        with pytest.raises(NotImplementedError):
            self.x.load_from_s3()

    def test_interface__load_from_file(self):
        with pytest.raises(NotImplementedError):
            self.x.load_from_file()

    def test_interface__load_from_filename(self):
        with pytest.raises(NotImplementedError):
            self.x.load_from_filename()

    def test_interface__load(self):
        with pytest.raises(NotImplementedError):
            self.x.load()

    def test_interface__clear(self):
        with pytest.raises(NotImplementedError):
            self.x.clear()
