import pytest
from cdx_toolkit.filter_cdx.matcher import TupleMatcher, TrieMatcher


@pytest.mark.parametrize(
    "prefixes,test_strings,expected_results",
    [
        # Basic functionality
        (
            ["http://", "https://"],
            ["http://example.com", "https://example.com", "ftp://example.com"],
            [True, True, False],
        ),
        # Empty prefix list
        ([], ["any string", "", "test"], [False, False, False]),
        # Single character prefixes
        (
            ["a", "b", "c"],
            ["apple", "banana", "cherry", "dog", ""],
            [True, True, True, False, False],
        ),
        # Overlapping prefixes
        (
            ["test", "testing", "te"],
            ["test", "testing", "tea", "other"],
            [True, True, True, False],
        ),
        # Unicode characters
        (
            ["café", "naïve", "résumé"],
            ["café au lait", "naïve person", "résumé.pdf", "regular text"],
            [True, True, True, False],
        ),
        # Special characters
        (
            ["[test]", ".*", "\\n"],
            ["[test] case", ".*regex", "\\newline", "normal"],
            [True, True, True, False],
        ),
        # Case sensitivity
        (
            ["HTTP", "Https"],
            ["HTTP://example.com", "https://example.com", "HTTPS://EXAMPLE.COM"],
            [True, False, True],
        ),
        # Very long prefixes
        (
            ["a" * 1000],
            ["a" * 1000 + "suffix", "a" * 999, "b" * 1000],
            [True, False, False],
        ),
        # Duplicate prefixes
        (
            ["test", "test", "demo"],
            ["testing", "demo version", "other"],
            [True, True, False],
        ),
        # Prefixes that are substrings of each other
        (
            ["ab", "abc", "abcd"],
            ["ab", "abc", "abcd", "abcde", "a"],
            [True, True, True, True, False],
        ),
        # Numbers and mixed content
        (
            ["123", "4.56"],
            ["123test", "4.56789", "789", "test123"],
            [True, True, False, False],
        ),
        # Whitespace handling (note: whitespace is stripped from prefixes, so " test" becomes "test")
        (
            [" test", "\tindent", "\nline"],
            ["test case", "indented", "line break", " test case", "nowhitespace"],
            [True, True, True, False, False],
        ),
    ],
)
def test_matcher_approaches(prefixes, test_strings, expected_results):
    """Test that TupleMatcher and TrieMatcher produce identical results."""
    tuple_matcher = TupleMatcher(prefixes)
    trie_matcher = TrieMatcher(prefixes)

    for test_string, expected_result in zip(test_strings, expected_results):
        tuple_result = tuple_matcher.matches(test_string)
        trie_result = trie_matcher.matches(test_string)

        # Both matchers should agree with each other
        assert tuple_result == trie_result, (
            f"TupleMatcher({tuple_result}) != TrieMatcher({trie_result}) "
            f"for prefixes {prefixes} and string '{test_string}'"
        )

        # Both should match the expected result
        assert tuple_result == expected_result, (
            f"Expected {expected_result}, got {tuple_result} "
            f"for prefixes {prefixes} and string '{test_string}'"
        )


@pytest.mark.parametrize(
    "invalid_prefixes,expected_error",
    [
        # Empty string prefixes
        ([""], "Empty prefixes are not allowed"),
        # Whitespace-only prefixes (should be stripped to empty and raise error)
        (["   "], "Empty prefixes are not allowed"),
        (["\t\n "], "Empty prefixes are not allowed"),
        # None values
        ([None], "Prefix must be a string and not none"),
        (["test", None, "demo"], "Prefix must be a string and not none"),
        # Non-string types
        ([123], "Prefix must be a string and not none"),
        (["test", 456, "demo"], "Prefix must be a string and not none"),
        ([[], {}, set()], "Prefix must be a string and not none"),
    ],
)
def test_prefix_validation_errors(invalid_prefixes, expected_error):
    """Test that invalid prefixes raise appropriate ValueErrors."""

    with pytest.raises(ValueError, match=expected_error):
        TupleMatcher(invalid_prefixes)

    with pytest.raises(ValueError, match=expected_error):
        TrieMatcher(invalid_prefixes)


@pytest.mark.parametrize(
    "test_string,expected",
    [
        ("test", True),
        ("testing", True),
        ("demo", True),
        ("demonstration", True),
        ("example", True),
        ("examples", True),
        ("  test", False),  # Leading whitespace in test string shouldn't match
        ("other", False),
    ],
)
def test_whitespace_stripping(test_string, expected):
    """Test that whitespace is properly stripped from prefixes."""

    # Prefixes with leading/trailing whitespace should be stripped
    prefixes_with_whitespace = ["  test  ", "\tdemo\n", " example "]

    tuple_matcher = TupleMatcher(prefixes_with_whitespace)
    trie_matcher = TrieMatcher(prefixes_with_whitespace)

    tuple_result = tuple_matcher.matches(test_string)
    trie_result = trie_matcher.matches(test_string)

    assert tuple_result == trie_result == expected, (
        f"Whitespace stripping test failed for '{test_string}': "
        f"expected {expected}, got Tuple({tuple_result}), Trie({trie_result})"
    )


@pytest.mark.parametrize("test_string", ["anything", "", "test", "a", "123"])
def test_empty_prefix_list(test_string):
    """Test with empty prefix list - should never match anything."""
    empty_prefixes = []

    tuple_matcher = TupleMatcher(empty_prefixes)
    trie_matcher = TrieMatcher(empty_prefixes)

    tuple_result = tuple_matcher.matches(test_string)
    trie_result = trie_matcher.matches(test_string)

    # Both should return False for empty prefix list
    assert tuple_result == trie_result == False, (
        f"Both matchers should return False for '{test_string}' with empty prefixes, "
        f"got Tuple({tuple_result}), Trie({trie_result})"
    )


def test_empty_string_against_prefixes():
    """Test matching empty strings against non-empty prefixes."""
    non_empty_prefixes = ["test", "demo", "example"]
    empty_test_string = ""

    tuple_matcher = TupleMatcher(non_empty_prefixes)
    trie_matcher = TrieMatcher(non_empty_prefixes)

    tuple_result = tuple_matcher.matches(empty_test_string)
    trie_result = trie_matcher.matches(empty_test_string)

    # Both should return False when testing empty string against non-empty prefixes
    assert tuple_result == trie_result == False, (
        f"Both matchers should return False for empty string with non-empty prefixes, "
        f"got Tuple({tuple_result}), Trie({trie_result})"
    )


@pytest.mark.parametrize(
    "test_string,expected",
    [
        ("a", True),
        ("1", True),
        ("!", True),
        ("ab", True),
        ("12", True),
        ("!@", True),
        ("other", False),
        ("", False),
    ],
)
def test_single_character_edge_cases(test_string, expected):
    """Test single character prefixes and strings (without empty prefixes)."""
    prefixes = ["a", "1", "!"]

    tuple_matcher = TupleMatcher(prefixes)
    trie_matcher = TrieMatcher(prefixes)

    tuple_result = tuple_matcher.matches(test_string)
    trie_result = trie_matcher.matches(test_string)

    assert (
        tuple_result == trie_result == expected
    ), f"Mismatch for '{test_string}': Tuple({tuple_result}), Trie({trie_result}), Expected({expected})"


def test_performance_with_many_prefixes():
    """Test with a large number of prefixes to ensure both matchers handle it."""
    # Create many prefixes
    prefixes = [f"prefix_{i}" for i in range(1000)]
    test_strings = ["prefix_500test", "prefix_999", "nomatch", "prefix_1000"]

    tuple_matcher = TupleMatcher(prefixes)
    trie_matcher = TrieMatcher(prefixes)

    for test_string in test_strings:
        tuple_result = tuple_matcher.matches(test_string)
        trie_result = trie_matcher.matches(test_string)
        assert tuple_result == trie_result


@pytest.mark.parametrize(
    "test_string,expected",
    [
        ("", False),
        ("a", True),
        ("ab", True),
        ("abc", True),
        ("abcd", True),
        ("abcde", True),
        ("abcdef", True),
        ("b", False),
        ("ac", True),
    ],
)
def test_nested_prefixes(test_string, expected):
    """Test with prefixes that are nested within each other."""
    prefixes = ["a", "ab", "abc", "abcd", "abcde"]

    tuple_matcher = TupleMatcher(prefixes)
    trie_matcher = TrieMatcher(prefixes)

    tuple_result = tuple_matcher.matches(test_string)
    trie_result = trie_matcher.matches(test_string)

    assert tuple_result == trie_result == expected, (
        f"Nested prefix test failed for '{test_string}': "
        f"expected {expected}, got Tuple({tuple_result}), Trie({trie_result})"
    )


@pytest.mark.parametrize(
    "test_string,expected",
    [
        ("🌟test", True),
        ("café au lait", True),
        ("𝓤𝓷𝓲𝓬𝓸𝓭𝓮 text", True),
        ("regular", False),
        ("", False),
    ],
)
def test_unicode_edge_cases(test_string, expected):
    """Test Unicode handling edge cases (without empty prefixes)."""
    prefixes = ["🌟", "café", "𝓤𝓷𝓲𝓬𝓸𝓭𝓮"]

    tuple_matcher = TupleMatcher(prefixes)
    trie_matcher = TrieMatcher(prefixes)

    tuple_result = tuple_matcher.matches(test_string)
    trie_result = trie_matcher.matches(test_string)

    assert (
        tuple_result == trie_result == expected
    ), f"Unicode mismatch for '{test_string}': Tuple({tuple_result}), Trie({trie_result}), Expected({expected})"


def test_with_list_and_tuple_inputs():
    """Test that both list and tuple inputs work identically."""
    prefixes_list = ["test", "demo", "example"]
    prefixes_tuple = ("test", "demo", "example")
    test_strings = ["testing", "demo version", "example.com", "other"]

    # Test with list input
    tuple_matcher_list = TupleMatcher(prefixes_list)
    trie_matcher_list = TrieMatcher(prefixes_list)

    # Test with tuple input
    tuple_matcher_tuple = TupleMatcher(prefixes_tuple)
    trie_matcher_tuple = TrieMatcher(prefixes_tuple)

    for test_string in test_strings:
        # All four matchers should give same result
        results = [
            tuple_matcher_list.matches(test_string),
            trie_matcher_list.matches(test_string),
            tuple_matcher_tuple.matches(test_string),
            trie_matcher_tuple.matches(test_string),
        ]

        assert all(
            r == results[0] for r in results
        ), f"Inconsistent results for '{test_string}': {results}"

