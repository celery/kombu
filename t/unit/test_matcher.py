from __future__ import annotations

import pytest

from kombu.matcher import (MatcherNotInstalled, fnmatch, match, register,
                           registry, rematch, unregister)


class test_Matcher:

    def test_register_match_unregister_matcher(self):
        register("test_matcher", rematch)
        registry.matcher_pattern_first.append("test_matcher")
        assert registry._matchers["test_matcher"] == rematch
        assert match("data", r"d.*", "test_matcher") is not None
        assert registry._default_matcher == fnmatch
        registry._set_default_matcher("test_matcher")
        assert registry._default_matcher == rematch
        unregister("test_matcher")
        assert "test_matcher" not in registry._matchers
        registry._set_default_matcher("glob")
        assert registry._default_matcher == fnmatch

    def test_unregister_matcher_not_registered(self):
        with pytest.raises(MatcherNotInstalled):
            unregister('notinstalled')

    def test_match_using_unregistered_matcher(self):
        with pytest.raises(MatcherNotInstalled):
            match("data", r"d.*", "notinstalled")
