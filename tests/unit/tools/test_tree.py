from unittest import TestCase

from nodalize.tools.tree import Tree, TreeNode


class TestTree(TestCase):
    class MyClass:
        def __init__(self, id, keys, parents):
            self.id = id
            self.keys = keys
            self.parents = parents

    def test_tree_no_error(self):
        elems = [
            TestTree.MyClass("a", [1, 2, 3], None),
            TestTree.MyClass("b", [4, 5], [1, 3]),
            TestTree.MyClass("c", [6, 7], [2]),
            TestTree.MyClass("d", [8], [3, 3, 6]),
            TestTree.MyClass("e", ["a"], ["b"]),
            TestTree.MyClass("f", ["b"], None),
        ]

        groups = Tree([TreeNode(e.keys, e.parents, e) for e in elems]).levels
        groups = [[n.id for n in group] for group in groups]

        self.assertEqual([["a", "f"], ["b", "c", "e"], ["d"]], groups)

    def test_tree_duplicated_keys(self):
        elems = [
            TestTree.MyClass("a", [1, 2, 3], None),
            TestTree.MyClass("b", [4, 5], [1, 3]),
            TestTree.MyClass("c", [6, 1], None),
        ]

        with self.assertRaises(ValueError) as error:
            _ = Tree([TreeNode(e.keys, e.parents, e) for e in elems])

        self.assertEqual(str(error.exception), "Duplicated keys across nodes: 1")

    def test_tree_missing_parent(self):
        elems = [
            TestTree.MyClass("a", [1, 2, 3], None),
            TestTree.MyClass("b", [4, 5], [1, 3]),
            TestTree.MyClass("c", [6, 7], [2, 8]),
        ]

        with self.assertRaises(ValueError) as error:
            _ = Tree([TreeNode(e.keys, e.parents, e) for e in elems])

        self.assertEqual(str(error.exception), "Parent node 8 not found")

    def test_tree_circular_reference(self):
        elems = [
            TestTree.MyClass("a", [1, 2, 3], [6]),
            TestTree.MyClass("b", [4, 5], [1, 3]),
            TestTree.MyClass("c", [6, 7], [4]),
        ]

        tree = Tree([TreeNode(e.keys, e.parents, e) for e in elems])
        with self.assertRaises(ValueError) as error:
            _ = tree.levels

        self.assertEqual(str(error.exception), "Circular reference detected with key 1")
