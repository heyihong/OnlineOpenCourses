#include <iostream>
#include <vector>

template<class T>
struct TreeNode {
    TreeNode() = default;

    TreeNode(const T & value_) : value(value_), left(NULL), right(NULL), father(NULL) {
    }

    virtual ~TreeNode() {
    }

    T value;
    TreeNode<T>* left;
    TreeNode<T>* right; 
    TreeNode<T>* father;
};

template<class T>
struct AvlTreeNode : public TreeNode<T> {
    AvlTreeNode() = default;

    AvlTreeNode(const T & value_) : TreeNode<T>(value_), height(0) {
    }

    int height;
};

template<class T>
class OrderedSetIterator {
public:
    typedef T ValueType;
    typedef T& Reference;
    typedef TreeNode<T>* NodePtr;
    typedef OrderedSetIterator<T> Self;

    OrderedSetIterator(NodePtr node_) : node(node_) {
    }

    bool operator!=(const Self& other) {
        return node != other.node;
    }

    Reference operator*() {
        return (*node).value;
    }
     
    Self& operator++() {
        if (node->right != NULL) {
            for (node = node->right; node->left != NULL; node = node->left); 
        } else {
           for (; node->father != NULL && node == node->father->right; node = node->father);
           if (node->father != NULL) {
               node = node->father;
           }
        }
        return *this;
    }

    Self operator++(int) {
        NodePtr result = node;
        ++(*this);
        return result;
    }

    Self& operator--() {
        if (node->right != NULL) {
            for (node = node->right; node->left != NULL; node = node->left); 
        } else {
           for (; node->father != NULL && node == node->father->right; node = node->father);
           if (node->father != NULL) {
               node = node->father;
           }
        }
        return *this;
    }

    Self operator--(int) {
        NodePtr result = node;
        --(*this);
        return result;
    }

private:
    NodePtr node;
};

template<class T> 
class OrderedSet {
protected:
    typedef T ValueType;
    typedef AvlTreeNode<T> AvlNodeType;
    typedef TreeNode<T>* NodePtr;
    typedef AvlTreeNode<T>* AvlNodePtr;
    typedef OrderedSetIterator<T> Iterator;
public:
    OrderedSet() : root(new AvlNodeType()) {
    }

    Iterator begin() const {
        NodePtr node = root;
        for (; node->left != NULL; node = node->left);
        return node;
    }
    
    Iterator end() const {
        return root;
    }
    
    void Insert(const T & value) {
        NodePtr node = new AvlNodeType(value); 
        if (root->left == NULL) {
            root->left = node;
            node->father = root;
            return;
        }
        for (NodePtr p = root->left;;) {
            if (value == p->value) {
                delete node;
                return;
            }
            if (value < p->value) {
                if (p->left == NULL) {
                    p->left = node;
                    node->father = p;
                    break;
                }
                p = p->left;
            } else {
                if (p->right == NULL) {
                    p->right = node;
                    node->father = p;
                    break;
                }
                p = p->right;
            }
        }
        Rebalance(node);
    }

    void Erase(const T & value) {
        
    }

private:
    int GetHeight(NodePtr p) {
        return p == NULL ? -1 : ((AvlNodePtr)p)->height;
    }

    void UpdateHeight(NodePtr p) {
        ((AvlNodePtr)p)->height = std::max(GetHeight(p->left), GetHeight(p->right)) + 1;
    }

    void LeftRotate(NodePtr p) {
        NodePtr t = p->right; 
        p->right = t->left;
        if (t->left != NULL) {
            t->left->father = p;
        }
        if (p->father->left == p) {
            p->father->left = t;
        } else {
            p->father->right = t;
        }
        t->father = p->father;
        t->left = p;
        p->father = t;
        UpdateHeight(p);
        UpdateHeight(t);
    }

    void RightRotate(NodePtr p) {
        NodePtr t = p->left; 
        p->left = t->right;
        if (t->right != NULL) {
            t->right->father = p;
        }
        if (p->father->left == p) {
            p->father->left = t;
        } else {
            p->father->right = t;
        }
        t->father = p->father;
        t->right = p;
        p->father = t;
        UpdateHeight(p);
        UpdateHeight(t);
    }

    void Rebalance(NodePtr node) {
        for (node = node->father; node != root; node = node->father) {
            UpdateHeight(node);
            int left_height = GetHeight(node->left);
            int right_height = GetHeight(node->right);
            if (left_height == right_height + 2) {
                if (GetHeight(node->left->left) == right_height) {
                    LeftRotate(node->left);
                } 
                RightRotate(node); 
            } else if (left_height + 2 == right_height) {
                if (GetHeight(node->right->right) == left_height) {
                    RightRotate(node->right);
                }
                LeftRotate(node);
            }
        }
    }

    NodePtr root;
};

int main() {
    OrderedSet<int> set;
    std::vector<int> values = {6, 2, 7, 1, 4, 5, 2, 2};
    for (int value : values) {
        set.Insert(value);
    }
    for (int value : set) {
        std::cout << value << std::endl;
    }
    return 0;
}
