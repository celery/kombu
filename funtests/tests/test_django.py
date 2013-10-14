from nose import SkipTest

from kombu.tests.case import redirect_stdouts

from funtests import transport


class test_django(transport.TransportCase):
    transport = 'django'
    prefix = 'django'
    event_loop_max = 10

    def before_connect(self):

        @redirect_stdouts
        def setup_django(stdout, stderr):
            try:
                import django  # noqa
            except ImportError:
                raise SkipTest('django not installed')
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
                    INSTALLED_APPS=('kombu.transport.django', ),
                )
            from django.core.management import call_command
            call_command('syncdb')

        setup_django()
