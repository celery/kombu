# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from sphinx_celery import conf

globals().update(conf.build_config(
    'kombu', __file__,
    project='Kombu',
    version_dev='4.0',
    version_stable='3.0',
    canonical_url='http://docs.kombu.me',
    webdomain='kombu.me',
    github_project='celery/kombu',
    author='Ask Solem & contributors',
    author_name='Ask Solem',
    copyright='2009-2016',
    publisher='Celery Project',
    html_logo='images/kombusmall.jpg',
    html_favicon='images/favicon.ico',
    html_prepend_sidebars=['sidebardonations.html'],
    extra_extensions=[],
    configure_django_settings={'DEBUG': False},
    apicheck_ignore_modules=[
        'kombu.entity',
        'kombu.messaging',
        'kombu.transport.django.management',
        'kombu.transport.django.management.commands',
        r'kombu.transport.django.migrations.*',
        r'kombu.transport.django.south_migrations.*',
        'kombu.async.aws.ext',
        'kombu.async.aws.sqs.ext',
        'kombu.transport.qpid_patches',
    ],
))
