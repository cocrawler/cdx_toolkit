from typing import List, Tuple, Union
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class Matcher(ABC):
    """Base class for all matching approaches."""

    @abstractmethod
    def __init__(self, prefixes: Union[Tuple[str], List[str]]):
        """Initialize the matcher with a list of prefixes."""
        pass

    @abstractmethod
    def matches(self, text: str) -> bool:
        """Check if text starts with any of the prefixes."""
        pass

    @staticmethod
    def validate_prefixes(prefixes: Union[Tuple[str], List[str]]) -> Tuple[str]:
        valid_prefixes = []

        for prefix in prefixes:
            if prefix is None or not isinstance(prefix, str):
                raise ValueError('Prefix must be a string and not none.')

            # remove white spaces
            prefix = prefix.strip()

            if len(prefix) == 0:
                raise ValueError('Empty prefixes are not allowed')

            valid_prefixes.append(prefix)

        return tuple(valid_prefixes)


class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end = False


class TrieMatcher(Matcher):
    """Trie-based matching approach."""

    def __init__(self, prefixes: Union[Tuple[str], List[str]]):
        logger.info(f'Building trie matcher based on {len(prefixes):,} inputs')
        self.root = self._build_trie(self.validate_prefixes(prefixes))

    def _build_trie(self, prefixes: Tuple[str]):
        """Build a trie from a collection of prefixes."""
        root = TrieNode()
        for prefix in prefixes:
            node = root
            for char in prefix:
                if char not in node.children:
                    node.children[char] = TrieNode()
                node = node.children[char]
            node.is_end = True
        return root

    def matches(self, text: str) -> bool:
        """Check if text starts with any prefix in the trie."""
        node = self.root
        for char in text:
            if char not in node.children:
                return False
            node = node.children[char]
            if node.is_end:
                return True
        return False


class TupleMatcher(Matcher):
    """Tuple-based matching approach using the built-in method `str.startswith`."""

    def __init__(self, prefixes: Union[Tuple[str], List[str]]):
        logger.info(f'Building Tuple matcher based on {len(prefixes):,} inputs')
        self.prefixes_Tuple = self.validate_prefixes(prefixes)

    def matches(self, text: str) -> bool:
        """Check if text starts with any prefix in the Tuple."""
        return text.startswith(self.prefixes_Tuple)
