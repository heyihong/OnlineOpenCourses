import bst

def height(node):
    if node is None:
        return -1
    else:
        return node.height

def size(node):
    if node is None:
        return 0
    else:
        return node.size

def update_stats(node):
    node.height = max(height(node.left), height(node.right)) + 1
    node.size = size(node.left) + size(node.right) + 1

class AVL(bst.BST):
    """
AVL binary search tree implementation.
Supports insert, find, and delete-min operations in O(lg n) time.
"""
    def left_rotate(self, x):
        y = x.right
        y.parent = x.parent
        if y.parent.left is x:
            y.parent.left = y
        elif y.parent.right is x:
            y.parent.right = y
        x.right = y.left
        if x.right is not None:
            x.right.parent = x
        y.left = x
        x.parent = y
        update_stats(x)
        update_stats(y)

    def right_rotate(self, x):
        y = x.left
        y.parent = x.parent
        if y.parent.left is x:
            y.parent.left = y
        elif y.parent.right is x:
            y.parent.right = y
        x.left = y.right
        if x.left is not None:
            x.left.parent = x
        y.right = x
        x.parent = y
        update_stats(x)
        update_stats(y)

    def insert(self, t):
        """Insert key t into this tree, modifying it in-place."""
        node = bst.BST.insert(self, t)
        self.rebalance(node)
        self.reroot()

    def rebalance(self, node):
        while node.key is not None:
            update_stats(node)
            if height(node.left) >= 2 + height(node.right):
                if height(node.left.left) >= height(node.left.right):
                    self.right_rotate(node)
                else:
                    self.left_rotate(node.left)
                    self.right_rotate(node)
            elif height(node.right) >= 2 + height(node.left):
                if height(node.right.right) >= height(node.right.left):
                    self.left_rotate(node)
                else:
                    self.right_rotate(node.right)
                    self.left_rotate(node)
            node = node.parent

    def delete(self, t):
        node = bst.BST.delete(self, t)
        self.rebalance(node.parent)
        self.reroot()

def test(args=None):
    bst.test(args, BSTtype=AVL)

if __name__ == '__main__': test()
