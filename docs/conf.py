# -*- coding: utf-8 -*-

import sys
import os

# If your extensions are in another directory, add it here. If the directory
# is relative to the documentation root, use os.path.abspath to make it
# absolute, like shown here.
sys.path.append(os.path.join(os.pardir, "tests"))
import kombu  # noqa

from django.conf import settings  # noqa
if not settings.configured:
    settings.configure()
try:
    from django import setup as django_setup
except ImportError:
    pass
else:
    django_setup()

# General configuration
# ---------------------

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.coverage',
    'sphinx.ext.viewcode',
    'sphinx.ext.pngmath',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['.templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'Kombu'
copyright = '2009-2016, Ask Solem'

intersphinx_mapping = {
    'python': ('http://docs.python.org/dev', None),
    'celery': ('http://docs.celeryproject.org/en/latest', None),
    'djcelery': ('http://django-celery.readthedocs.org/en/latest', None),
    'cyme': ('http://cyme.readthedocs.org/en/latest', None),
    'amqp': ('http://amqp.readthedocs.org/en/latest', None),
    'vine': ('http://vine.readthedocs.org/en/latest', None),
    'redis': ('http://redis-py.readthedocs.org/en/latest', None),
    'django': ('http://django.readthedocs.org/en/latest', None),
    'boto': ('http://boto.readthedocs.org/en/latest', None),
    'sqlalchemy': ('http://sqlalchemy.readthedocs.org/en/latest', None),
    'kazoo': ('http://kazoo.readthedocs.org/en/latest', None),
    'pyzmq': ('http://pyzmq.readthedocs.org/en/latest', None),
    'msgpack': ('http://pythonhosted.org/msgpack-python/', None),
    'sphinx': ('http://www.sphinx-doc.org/en/stable/', None),
}

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = ".".join(map(str, kombu.VERSION[0:2]))
# The full version, including alpha/beta/rc tags.
release = kombu.__version__

exclude_trees = ['.build']

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'colorful'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['.static']

html_use_smartypants = True

# If false, no module index is generated.
html_use_modindex = True

# If false, no index is generated.
html_use_index = True

latex_documents = [
    ('index', 'Kombu.tex', 'Kombu Documentation',
     'Ask Solem', 'manual'),
]

html_theme = "celery"
html_theme_path = ["_theme"]
html_sidebars = {
    'index': ['sidebarintro.html', 'sourcelink.html', 'searchbox.html'],
    '**': ['sidebarlogo.html', 'localtoc.html', 'relations.html',
           'sourcelink.html', 'searchbox.html'],
}
