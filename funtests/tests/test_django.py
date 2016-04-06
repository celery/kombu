from __future__ import absolute_import, unicode_literals

from funtests import transport

from kombu.tests.case import mock, skip


@skip.unless_module('django')
class test_django(transport.TransportCase):
    transport = 'django'
    prefix = 'django'
    event_loop_max = 10

    @mock.stdouts
    def before_connect(self, stdout, stderr):
        from django.conf import settings
        if not settings.configured:
            settings.configure(
                DATABASE_ENGINE='sqlite3',
                DATABASE_NAME=':memory:',
                DATABASES={
                    'default': {
                        'ENGINE': 'django.db.backends.sqlite3',
                        'NAME': ':memory:',
                    },
                },
                INSTALLED_APPS=('kombu.transport.django',),
            )
        from django.core.management import call_command
        call_command('syncdb')
