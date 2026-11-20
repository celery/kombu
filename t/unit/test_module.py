from __future__ import annotations

import sys
import types
from pathlib import Path

import kombu


def _load_package_init(module_name: str, **names: object):
    source = Path(kombu.__file__).read_text(encoding='utf-8')
    placeholder = types.ModuleType(module_name)
    sys.modules[module_name] = placeholder
    namespace = placeholder.__dict__
    namespace['__name__'] = module_name
    namespace['__doc__'] = None
    namespace['__package__'] = module_name
    namespace.update(names)
    exec(compile(source, 'kombu/__init__.py', 'exec'), namespace)
    return sys.modules[module_name]


class test_package_import:

    def test_normal_import_keeps_file_and_path(self):
        assert isinstance(kombu.__file__, str)
        assert kombu.__path__
        assert '__file__' in dir(kombu)
        assert '__path__' in dir(kombu)
        assert kombu.__version__
        assert not hasattr(kombu, '_copied')
        assert kombu.Connection.__module__ == 'kombu.connection'

    def test_missing_path_raises(self):
        name = 'kombu_import_without_path'
        try:
            try:
                _load_package_init(name)
            except NameError as exc:
                assert '__path__' in str(exc)
            else:
                raise AssertionError('missing __path__ did not raise')
        finally:
            sys.modules.pop(name, None)

    def test_path_without_file(self):
        name = 'kombu_import_path_only'
        try:
            loaded = _load_package_init(name, __path__=['only-path'])
        finally:
            sys.modules.pop(name, None)
        assert not hasattr(loaded, '__file__')
        assert list(loaded.__path__) == ['only-path']
        assert '__file__' not in dir(loaded)
        assert '__path__' in dir(loaded)

    def test_file_without_path_raises(self):
        name = 'kombu_import_file_only'
        try:
            try:
                _load_package_init(name, __file__='only.py')
            except NameError as exc:
                assert '__path__' in str(exc)
            else:
                raise AssertionError('missing __path__ did not raise')
        finally:
            sys.modules.pop(name, None)

    def test_package_without_file_loads_connection(self):
        saved = {
            name: module
            for name, module in list(sys.modules.items())
            if name == 'kombu' or name.startswith('kombu.')
        }
        package_path = str(Path(kombu.__file__).parent)
        try:
            for name in list(saved):
                del sys.modules[name]
            loaded = _load_package_init('kombu', __path__=[package_path])
            assert not hasattr(loaded, '__file__')
            assert list(loaded.__path__) == [package_path]
            assert loaded.Connection.__module__ == 'kombu.connection'
            assert '__file__' not in dir(loaded)
            assert '__path__' in dir(loaded)
        finally:
            for name in list(sys.modules):
                if name == 'kombu' or name.startswith('kombu.'):
                    del sys.modules[name]
            sys.modules.update(saved)

    def test_present_file_and_path_are_copied(self):
        name = 'kombu_import_with_file'
        try:
            loaded = _load_package_init(
                name, __file__='frozen.py', __path__=['pkg'],
            )
        finally:
            sys.modules.pop(name, None)
        assert loaded.__file__ == 'frozen.py'
        assert list(loaded.__path__) == ['pkg']

    def test_explicit_none_file_and_path_are_copied(self):
        name = 'kombu_import_none_file'
        try:
            loaded = _load_package_init(name, __file__=None, __path__=None)
        finally:
            sys.modules.pop(name, None)
        assert loaded.__file__ is None
        assert loaded.__path__ is None

    def test_empty_file_is_kept(self):
        name = 'kombu_import_empty_file'
        try:
            loaded = _load_package_init(name, __file__='', __path__=[])
        finally:
            sys.modules.pop(name, None)
        assert loaded.__file__ == ''
        assert list(loaded.__path__) == []

    def test_real_module_unchanged(self):
        name = 'kombu_import_without_file_isolated'
        original = sys.modules['kombu']
        original_file = kombu.__file__
        try:
            try:
                _load_package_init(name)
            except NameError:
                pass
            else:
                raise AssertionError('missing __path__ did not raise')
        finally:
            sys.modules.pop(name, None)
        assert sys.modules['kombu'] is original
        assert kombu.__file__ == original_file
