"""Tools to build and manage trees."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, Generic, Iterable, List, Optional, Set, TypeVar

T = TypeVar("T")


class TreeNode(Generic[T]):
    """Node class."""

    def __init__(self, keys: List[str], parents: List[str], target: T) -> None:
        """
        Initialize node.

        Args:
            keys: list of keys that would identify this node, must belong to one node only
            parents: list of parents (matching other keys)
            target: underlying object
        """
        self.keys = keys
        self.parent_keys = parents or []
        self.target = target
        self.parent_nodes = []  # type: List[TreeNode]
        self._level = None  # type: Optional[int]

    def add_parent_node(self, parent: TreeNode[T]) -> None:
        """
        Add link to parent node.

        Args:
            parent: parent node
        """
        self.parent_nodes.append(parent)

    def _compute_level(self, explored_nodes: Optional[Set[str]] = None) -> int:
        """
        Return level, 0 means bottom leaf.

        Returns:
            level
        """
        if self._level is None:
            if explored_nodes is None:
                explored_nodes = set()

            for key in self.keys:
                if key in explored_nodes:
                    raise ValueError(f"Circular reference detected with key {key}")

            explored_nodes = explored_nodes.union(self.keys)

            if len(self.parent_nodes) == 0:
                self._level = 1
            else:
                self._level = 1 + max(
                    [p._compute_level(explored_nodes) for p in self.parent_nodes]
                )
        return self._level

    @property
    def level(self) -> int:
        """
        Return level, 0 means bottom leaf.

        Returns:
            level
        """
        return self._compute_level()


class Tree(Generic[T]):
    """Tree class."""

    def __init__(
        self,
        nodes: Iterable[TreeNode[T]],
        allow_key_duplicates: bool = False,
        skip_missing_parents: bool = False,
    ) -> None:
        """
        Initialize.

        Args:
            nodes: list of nodes
            allow_key_duplicates: if True then allow to have multiple nodes with the same key
            skip_missing_parents: if True, then don't fail when parent node is missing and move on
        """
        self._nodes = list(nodes)
        self._nodes_dict = {}  # type: Dict[str, TreeNode[T]]

        # Add the nodes to the dictionary
        for node in nodes:
            for key in node.keys:
                if key in self._nodes_dict.keys() and not allow_key_duplicates:
                    raise ValueError(f"Duplicated keys across nodes: {key}")
                else:
                    self._nodes_dict[key] = node

        # Link the nodes
        for node in self._nodes_dict.values():
            for parent_key in node.parent_keys:
                parent_node = self._nodes_dict.get(parent_key)

                if parent_node is not None:
                    node.add_parent_node(parent_node)
                elif not skip_missing_parents:
                    raise ValueError(f"Parent node {parent_key} not found")

    @property
    def levels(self) -> List[List[T]]:
        """
        Get ordered levels.

        Returns:
            ordered list of list - each sub-list contains items of the same level
        """
        # Compute levels
        levels = defaultdict(list)
        for node in self._nodes:
            levels[node.level].append(node)

        # Order the groups
        if len(levels) > 0:
            groups = []
            for level in range(1, max(levels.keys()) + 1):
                elems = [node.target for node in levels[level]]
                if len(elems) > 0:
                    groups.append(elems)
            return groups
        else:
            return []
