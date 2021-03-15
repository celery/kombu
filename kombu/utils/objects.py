"""Object Utilities."""


try:
    from functools import cached_property
except ImportError:
    # TODO: Remove this fallback once we drop support for Python < 3.8
    from cached_property import threaded_cached_property as cached_property
