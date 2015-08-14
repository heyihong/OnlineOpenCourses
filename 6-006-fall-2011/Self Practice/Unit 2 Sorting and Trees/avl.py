import bst

def height(node):
    if node:
        return node.height
    return -1

def update_height(node):
    node.height = max(height(node.left), height(node.right)) + 1 

class AVL(bst.BST):
    def left_rotate(self, x):  
        y = x.right
        y.father = x.father
        if y.father:
            if y.father.left == x:
                y.father.left = y
            else:
                y.father.right = y
        else:
            self.root = y    
        x.right = y.left
        if x.right:
            y.left.father = x
        y.left = x
        x.father = y
        update_height(x)
        update_height(y)

    def right_rotate(self, x):
        y = x.left
        y.father = x.father
        if y.father:
            if y.father.left == x:
                y.father.left = y
            else:
                y.father.right = y
        else:
            self.root = y
        x.left = y.right
        if x.left:
            y.right.father = x
        y.right = x
        x.father = y
        update_height(x)
        update_height(y)
         
    def insert(self, key):
        node = bst.BST.insert(self, key)
        self.rebalance(node)

    def delete(self, key):
        node = bst.BST.delete(self, key)
        self.rebalance(node.father)

    def rebalance(self, node):
        while node:
            update_height(node)
            if height(node.left) == height(node.right) - 2:   
                if height(node.right.right) == height(node.left):
                    self.right_rotate(node.right)
                self.left_rotate(node)
            elif height(node.right) == height(node.left) - 2:
                if height(node.left.left) == height(node.right):
                    self.left_rotate(node.left)
                self.right_rotate(node)
            node = node.father

if __name__ == "__main__":
    bst.test(None, AVL)
