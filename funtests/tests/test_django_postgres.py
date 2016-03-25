from nose import SkipTest

from kombu.tests.case import redirect_stdouts

from funtests import transport


class test_django_postgres(transport.TransportCase):
    transport = 'django_postgres'
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
                    DATABASES={
                        'default': {
                            'ENGINE': 'django.db.backends.postgresql_psycopg2',
                            'NAME': 'kombu',
                        },
                    },
                    INSTALLED_APPS=('kombu.transport.django',),
                    MIDDLEWARE_CLASSES=(),
                )
            from django.core.management import call_command
            try:
                # new in Django 1.7
                setup = django.setup
            except AttributeError:
                pass
            else:
                setup()

            call_command('syncdb')

        setup_django()
