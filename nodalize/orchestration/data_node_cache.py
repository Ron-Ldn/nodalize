"""Cache for data nodes."""

from typing import Dict, List

from nodalize.datanode import DataNode
from nodalize.dependency import DependencyDefinition
from nodalize.tools.tree import Tree, TreeNode


class DataNodeCache:
    """Cache for data nodes."""

    def __init__(self) -> None:
        """Initialize."""
        self._nodes = {}  # type: Dict[str, DataNode]
        self._tree_nodes = {}  # type: Dict[str, TreeNode[DataNode]]
        self._downstream_dependencies = {}  # type: Dict[str, List[str]]

    def add_node(self, node: DataNode) -> None:
        """
        Add node to cache.

        Args:
            node: data node
        """
        if node.identifier in self._nodes:
            raise AssertionError(f"{node.identifier} is defined multiple times")
        else:
            self._nodes[node.identifier] = node

    def build_dependency_graph(self) -> None:
        """Build dependency graph."""
        for data_node in self._nodes.values():
            # Build dependencies
            dependencies = {}  # type: Dict[str, DependencyDefinition]
            for key, raw_dep in data_node.dependencies.items():
                if raw_dep is None:
                    dep_node = self._nodes[key]
                    dep = dep_node.build_dependency()
                    dependencies[key] = dep
                elif isinstance(raw_dep, str):
                    dep_node = self._nodes[raw_dep]
                    dep = dep_node.build_dependency()
                    dependencies[key] = dep
                elif isinstance(raw_dep, DependencyDefinition):
                    raw_dep.assign_data_node(self._nodes[raw_dep.data_node_identifier])
                    dependencies[key] = raw_dep
                else:
                    raise ValueError(
                        f"Unknown class type for dependency: {raw_dep.__class__.__name__}"
                    )
            data_node.set_dependencies(dependencies)

        for data_node in self._nodes.values():
            parents = [
                dep.data_node_identifier
                for dep in data_node.get_dependencies().values()
            ]
            node = TreeNode([data_node.identifier], parents, data_node)
            self._tree_nodes[data_node.identifier] = node

            for p in parents:
                children = self._downstream_dependencies.get(p, [])
                children.append(data_node.identifier)
                self._downstream_dependencies[p] = children

    def get_data_node(self, identifier: str) -> DataNode:
        """
        Get data node definition.

        Args:
            identifier: identifier of the data node to find

        Returns:
            data node definition object
        """
        data_node = self._tree_nodes.get(identifier)

        if data_node is None:
            raise KeyError(f"Data node {identifier} could not be found")

        return data_node.target

    def build_downstream_tree(self, required_data_nodes: List[str]) -> Tree[DataNode]:
        """
        Build data node dependency tree.

        Args:
            required_data_nodes: list of data nodes to include in the graph

        Returns:
            tree with the required data nodes and their downstream dependencies
        """
        nodes = {}

        def add_tree_node(id: str) -> None:
            if id not in nodes:
                node = self._tree_nodes.get(id)
                if node is None:
                    raise ValueError(f"Data node not found: {id}")

                nodes[id] = node

                for child in self._downstream_dependencies.get(id, []):
                    add_tree_node(child)

        for node in required_data_nodes:
            add_tree_node(node)

        return Tree(nodes.values(), skip_missing_parents=True)
