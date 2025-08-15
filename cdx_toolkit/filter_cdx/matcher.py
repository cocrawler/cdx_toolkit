import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class Matcher(ABC):
    """Base class for all matching approaches."""

    @abstractmethod
    def __init__(self, prefixes: tuple[str] | list[str]):
        """Initialize the matcher with a list of prefixes."""
        pass

    @abstractmethod
    def matches(self, text: str) -> bool:
        """Check if text starts with any of the prefixes."""
        pass


class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end = False


class TrieMatcher(Matcher):
    """Trie-based matching approach."""

    def __init__(self, prefixes: tuple[str] | list[str]):
        logger.info(f"Building trie matcher based on {len(prefixes):,} inputs")
        self.root = self._build_trie(prefixes)

    def _build_trie(self, prefixes: tuple[str] | list[str]):
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
    """Tuple-based matching approach using startswith."""

    def __init__(self, prefixes: tuple[str] | list[str]):
        logger.info(f"Building tuple matcher based on {len(prefixes):,} inputs")
        self.prefixes_tuple = tuple(prefixes)

    def matches(self, text: str) -> bool:
        """Check if text starts with any prefix in the tuple."""
        return text.startswith(self.prefixes_tuple)
