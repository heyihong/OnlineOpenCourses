
class BST(object):
    
    def __init__(self):
        self.root = None

    def insert(self, key):
        newNode = BSTnode(key)
        if not self.root:
            self.root = newNode
        else:
            node = self.root
            while True:
                if key < node.key:
                    if (node.left == None):
                        node.left = newNode
                        newNode.father = node
                        break
                    node = node.left 
                else:
                    if (node.right == None):
                        node.right = newNode
                        newNode.father = node
                        break
                    node = node.right
        return newNode
        

    def delete(self, key):
        node = self.find(key)
        if node:
            #find out the node can be easily deleted which has at most one child
            if node.right:
                succ = node.right
                while succ.left:
                    succ = succ.left 
                (node.key, succ.key) = (succ.key, node.key)
                node = succ
                child = node.right
            else:
                child = node.left
            #detele the node
            if not node.father:
                self.root = child 
            elif node.father.left == node:
                node.father.left = child
            else:
                node.father.right = child  
            if child:
                child.father = node.father
        return node


    def find(self, key):
        node = self.root
        while node:
            if key < node.key:
                node = node.left
            elif key > node.key:
                node = node.right
            else:
                break
        return node

    def __str__(self):
        if not self.root:
            return ""
        else:
            return str(self.root)


class BSTnode(object):

    def __init__(self, key):
        self.left = self.right = self.father = None
        self.key = key

    def __str__(self): 
        ret = ""
        if self.left:
            ret = ret + str(self.left) + ", "
        ret = ret + str(self.key) 
        if self.right:
            ret = ret + ", " + str(self.right)
        return ret 

def test(args = None, BSTtype = BST):
    import random, sys
    if not args: 
        args = sys.argv[1:]
    if not args:
        print 'usage: %s <number-of-random-items | item item item ...>' % sys.argv[0]
        sys.exit()
    if len(args) == 1:
        elements = [random.randrange(100) for i in xrange(int(args[0]))]
    else:
        elements = [int(element) for element in args]

    tree = BSTtype()
    for element in elements:
        print "insert element:", element
        tree.insert(element)        
        print "inorder trasversal:", tree
    for element in elements:
        print "delete element:", element
        tree.delete(element)
        print "inorder trasversal:", tree
    
if __name__ == "__main__":
    test()
