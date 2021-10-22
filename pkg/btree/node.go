package btree

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"

	pager "github.com/brown-csci1270/db/pkg/pager"
)

// Split is a supporting data structure to propagate keys up our B+ tree.
type Split struct {
	isSplit bool  // A flag that's set if a split occurs.
	key     int64 // The key to promote.
	leftPN  int64 // The pagenumber for the left node.
	rightPN int64 // The pagenumber for the right node.
	err     error // Used to propagate errors upwards.
}

// Node defines a common interface for leaf and internal nodes.
type Node interface {
	// Interface for main node functions.
	search(int64) int64
	insert(int64, int64, bool) Split
	delete(int64)
	get(int64) (int64, bool)

	// Interface for helper functions.
	keyToNodeEntry(int64) (*LeafNode, int64, error)
	printNode(io.Writer, string, string)
	getPage() *pager.Page
	getNodeType() NodeType
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////// Leaf Node Methods /////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key >= given key.
// If no key satisfies this condition, returns numKeys.
func (node *LeafNode) search(key int64) int64 {
	idx := sort.Search(int(node.numKeys), func(i int) bool {
		return node.getKeyAt(int64(i)) >= key
	})
	return int64(idx)
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
// if update is true, allow overwriting existing keys. else, error.
func (node *LeafNode) insert(key int64, value int64, update bool) Split {
	// search the place to insert the tuple
	idx := node.search(key)
	if update {
		// update only if idx is in range and the searched result is equal to key
		if idx < node.numKeys && node.getKeyAt(idx) == key {
			// update the node only if 'update' is true
			node.updateValueAt(idx, value)
		} else {
			return Split{false, 0, 0, 0, errors.New("cannot update non-existing tuple")}
		}
	} else {
		if idx < node.numKeys && node.getKeyAt(idx) == key {
			// the key already exist, throw an error.
			return Split{false, 0, 0, 0, errors.New("cannot insert existing key")}
		} else {
			// shift the existing tuples if needed
			for i := node.numKeys - 1; i >= idx; i -= 1 {
				node.updateValueAt(i+1, node.getValueAt(i))
				node.updateKeyAt(i+1, node.getKeyAt(i))
			}

			// insert the tuple and increase the numKeys by 1
			node.updateKeyAt(idx, key)
			node.updateValueAt(idx, value)
			node.updateNumKeys(node.numKeys + 1)
		}
	}
	// split and propagate the split if needed
	return node.split()
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *LeafNode) delete(key int64) {
	index := node.search(key)
	if index >= node.numKeys || node.getKeyAt(index) != key {
		// no need for deletion
		return
	}

	// delete the value by shifting all the tuples on the right side of index to left by one
	for i := index + 1; i < node.numKeys; i++ {
		node.updateKeyAt(i-1, node.getKeyAt(i))
		node.updateValueAt(i-1, node.getValueAt(i))
	}
	// decrease the numKeys
	node.updateNumKeys(node.numKeys - 1)
	return
}

// split is a helper function to split a leaf node, then propagate the split upwards.
func (node *LeafNode) split() Split {
	var ret Split

	if node.numKeys < ENTRIES_PER_LEAF_NODE {
		// no need for split
		ret.isSplit = false
	} else {
		// create a new leaf node
		newRight, err := createLeafNode(node.page.GetPager())
		if err != nil {
			ret.err = err
			return ret
		}
		defer newRight.getPage().Put()

		half := node.numKeys / 2

		// copy from median to numKeys - 1 to the newRight
		for i := half; i < node.numKeys; i++ {
			newRight.updateKeyAt(i-half, node.getKeyAt(i))
			newRight.updateValueAt(i-half, node.getValueAt(i))
		}
		// update the number of keys in the newNode
		newRight.updateNumKeys(node.numKeys - half)
		// set newRight's right sibling to point to oldNode's right sibling
		newRight.setRightSibling(node.rightSiblingPN)

		// set the numKeys of current node to half, (which serves as deletion of tuples)
		node.updateNumKeys(half)
		// set the new node as the right sibling of the old node
		node.setRightSibling(newRight.getPage().GetPageNum())

		ret.isSplit = true
		ret.key = newRight.getKeyAt(0)
		ret.leftPN = node.getPage().GetPageNum()
		ret.rightPN = newRight.getPage().GetPageNum()
		ret.err = nil
	}
	return ret
}

// get returns the value associated with a given key from the leaf node.
func (node *LeafNode) get(key int64) (value int64, found bool) {
	index := node.search(key)
	if index >= node.numKeys || node.getKeyAt(index) != key {
		// Thank you Mario! But our key is in another castle!
		return 0, false
	}
	entry := node.getCell(index)
	return entry.GetValue(), true
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *LeafNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	return node, node.search(key), nil
}

// printNode pretty prints our leaf node.
func (node *LeafNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Leaf"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	for cellnum := int64(0); cellnum < node.numKeys; cellnum++ {
		entry := node.getCell(cellnum)
		io.WriteString(w, fmt.Sprintf("%v |--> (%v, %v)\n",
			prefix, entry.GetKey(), entry.GetValue()))
	}
	if node.rightSiblingPN > 0 {
		io.WriteString(w, fmt.Sprintf("%v |--+\n", prefix))
		io.WriteString(w, fmt.Sprintf("%v    | node @ %v\n",
			prefix, node.rightSiblingPN))
		io.WriteString(w, fmt.Sprintf("%v    v\n", prefix))
	}
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////// Internal Node Methods ///////////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key > given key.
// If no such index exists, it returns numKeys.
func (node *InternalNode) search(key int64) int64 {
	idx := sort.Search(int(node.numKeys), func(i int) bool {
		return node.getKeyAt(int64(i)) > key
	})
	return int64(idx)
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
func (node *InternalNode) insert(key int64, value int64, update bool) Split {
	idx := node.search(key)
	child, err := node.getChildAt(idx)
	if err != nil {
		return Split{false, 0, 0, 0, err}
	}
	defer child.getPage().Put()

	split := child.insert(key, value, update)
	if split.isSplit {
		split = node.insertSplit(split)
	}
	return split
}

// insertSplit inserts a split result into an internal node.
// If this insertion results in another split, the split is cascaded upwards.
func (node *InternalNode) insertSplit(split Split) Split {
	if !split.isSplit {
		// no need for insertion
		return Split{false, 0, 0, 0, split.err}
	}

	idx := node.search(split.key)

	// first deal with the special case for interior nodes
	node.updatePNAt(node.numKeys+1, node.getPNAt(node.numKeys))
	// shift all the info from idx to numKeys - 1  to right by one
	for i := node.numKeys - 1; i >= idx; i -= 1 {
		node.updateKeyAt(i+1, node.getKeyAt(i))
		node.updatePNAt(i+1, node.getPNAt(i))
	}

	// update the info at idx and increase numKeys
	node.updateKeyAt(idx, split.key)
	node.updateNumKeys(node.numKeys + 1)

	// update the pointers to children
	node.updatePNAt(idx, split.leftPN)
	node.updatePNAt(idx+1, split.rightPN)

	// split the internal node again if needed
	return node.split()
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *InternalNode) delete(key int64) {
	idx := node.search(key)
	child, err := node.getChildAt(idx)
	if err != nil {
		return
	}
	defer child.getPage().Put()

	child.delete(key)
	return
}

// split is a helper function that splits an internal node, then propagates the split upwards.
func (node *InternalNode) split() Split {
	var ret Split

	if node.numKeys < ENTRIES_PER_LEAF_NODE {
		// No need to split
		ret.isSplit = false
	} else {
		// create new internal node
		newRight, err := createInternalNode(node.page.GetPager())
		if err != nil {
			ret.err = err
			return ret
		}
		defer newRight.getPage().Put()
		// set up the key to propagate
		half := node.numKeys / 2
		ret.key = node.getKeyAt(half)

		rightStart := half + 1
		// copy from median to numKey - 1 to the newRight
		for i := rightStart; i < node.numKeys; i++ {
			newRight.updateKeyAt(i-rightStart, node.getKeyAt(i))
			newRight.updatePNAt(i-rightStart, node.getPNAt(i))
		}
		// we also need to copy the last pagenum, which is at idx = numKeys
		newRight.updatePNAt(node.numKeys-rightStart, node.getPNAt(node.numKeys))

		// update the number of tuples in the newNodes
		newRight.updateNumKeys(node.numKeys - half - 1)

		// set the numKeys of current node to half, (which serves as deletion of tuples)
		node.updateNumKeys(half)

		ret.isSplit = true
		ret.leftPN = node.getPage().GetPageNum()
		ret.rightPN = newRight.getPage().GetPageNum()
		ret.err = nil
	}
	return ret
}

// get returns the value associated with a given key from the leaf node.
func (node *InternalNode) get(key int64) (value int64, found bool) {
	childIdx := node.search(key)
	child, err := node.getChildAt(childIdx)
	if err != nil {
		return 0, false
	}
	defer child.getPage().Put()
	return child.get(key)
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *InternalNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	index := node.search(key)
	child, err := node.getChildAt(index)
	if err != nil {
		return &LeafNode{}, 0, err
	}
	defer child.getPage().Put()
	return child.keyToNodeEntry(key)
}

// printNode pretty prints our internal node.
func (node *InternalNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Internal"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys + 1))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	nextFirstPrefix := prefix + " |--> "
	nextPrefix := prefix + " |    "
	for idx := int64(0); idx <= node.numKeys; idx++ {
		io.WriteString(w, fmt.Sprintf("%v\n", nextPrefix))
		child, err := node.getChildAt(idx)
		if err != nil {
			return
		}
		defer child.getPage().Put()
		child.printNode(w, nextFirstPrefix, nextPrefix)
	}
}
