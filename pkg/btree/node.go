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
	/* SOLUTION {{{ */
	// Binary search for the key.
	minIndex := sort.Search(
		int(node.numKeys),
		func(idx int) bool {
			return node.getKeyAt(int64(idx)) >= key
		},
	)
	return int64(minIndex)
	/* SOLUTION }}} */
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
// if update is true, allow overwriting existing keys. else, error.
func (node *LeafNode) insert(key int64, value int64, update bool) Split {
	node.unlockParent(false)
	defer node.unlock()

	// search the place to insert the tuple
	idx := node.search(key)

	// update will not cause any split, thus should be handled separately
	if update {
		// when updating, no split can possibly happen, hence no need to hold the locks for parent nodes
		// check if idx is in range and the searched result is equal to key
		node.unlockParent(true)
		if idx >= node.numKeys || node.getKeyAt(idx) != key {
			return Split{isSplit: false, err: errors.New("cannot update non-existing tuple")}
		}

		node.updateValueAt(idx, value)
		return Split{isSplit: false}
	}

	// HANDLE INSERTION
	if idx < node.numKeys && node.getKeyAt(idx) == key {
		// the key already exists, throw an error.
		// since no further split can happen, unlock all the parent nodes
		node.unlockParent(true)
		return Split{isSplit: false, err: errors.New("cannot insert existing key")}
	}

	// shift the existing tuples if needed
	for i := node.numKeys - 1; i >= idx; i -= 1 {
		node.updateValueAt(i+1, node.getValueAt(i))
		node.updateKeyAt(i+1, node.getKeyAt(i))
	}

	// insert the tuple and increase the numKeys by 1
	node.updateKeyAt(idx, key)
	node.updateValueAt(idx, value)
	node.updateNumKeys(node.numKeys + 1)

	// split and propagate the split
	split := node.split()

	if !split.isSplit {
		// if no split happens, unlock all the parents
		// Otherwise, the parents should remain locked and let them unlock themselves
		node.unlockParent(true)
	}

	return split
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *LeafNode) delete(key int64) {
	/* SOLUTION {{{ */
	node.unlockParent(true)
	defer node.unlock()
	// Find entry.
	deletePos := node.search(key)
	if deletePos >= node.numKeys || node.getKeyAt(deletePos) != key {
		// Thank you Mario! But our key is in another castle!
		return
	}
	// Shift entries to the left.
	for i := deletePos; i < node.numKeys-1; i++ {
		node.updateKeyAt(i, node.getKeyAt(i+1))
		node.updateValueAt(i, node.getValueAt(i+1))
	}
	node.updateNumKeys(node.numKeys - 1)
	/* SOLUTION }}} */
}

// split is a helper function to split a leaf node, then propagate the split upwards.
func (node *LeafNode) split() Split {
	/* SOLUTION {{{ */
	// Create a new leaf node to split our keys.
	newNode, err := createLeafNode(node.page.GetPager())
	if err != nil {
		return Split{err: err}
	}
	defer newNode.getPage().Put()
	// Set the right sibling for our two nodes.
	prevSiblingPN := node.setRightSibling(newNode.page.GetPageNum())
	newNode.setRightSibling(prevSiblingPN)
	// Transfer entries to the new node (plus the new entry) accordingly.
	midpoint := node.numKeys / 2
	for i := midpoint; i < node.numKeys; i++ {
		newNode.updateKeyAt(newNode.numKeys, node.getKeyAt(i))
		newNode.updateValueAt(newNode.numKeys, node.getValueAt(i))
		newNode.updateNumKeys(newNode.numKeys + 1)
	}
	node.updateNumKeys(midpoint)
	return Split{
		isSplit: true,
		key:     newNode.getKeyAt(0), // Get the right node's first key
		leftPN:  node.page.GetPageNum(),
		rightPN: newNode.page.GetPageNum(),
	}
	/* SOLUTION }}} */
}

// get returns the value associated with a given key from the leaf node.
func (node *LeafNode) get(key int64) (value int64, found bool) {
	// Unlock parents, eventually unlock this node.
	node.unlockParent(true)
	defer node.unlock()
	// Find index.
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
		io.WriteString(w, fmt.Sprintf("%v    | right sibling @ [%v]\n",
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
	/* SOLUTION {{{ */
	// Binary search for the key.
	minIndex := sort.Search(
		int(node.numKeys),
		func(idx int) bool {
			return node.getKeyAt(int64(idx)) > key
		},
	)
	return int64(minIndex)
	/* SOLUTION }}} */
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
func (node *InternalNode) insert(key int64, value int64, update bool) Split {
	/* SOLUTION {{{ */
	// Insert the entry into the appropriate child node.
	node.unlockParent(false)
	childIdx := node.search(key)
	child, err := node.getChildAt(childIdx, true)
	if err != nil {
		node.unlockParent(true)
		node.unlock()
		return Split{err: err}
	}
	defer child.getPage().Put()
	node.initChild(child)
	// Insert value into the child.
	result := child.insert(key, value, update)
	// Insert a new key into our node if necessary.
	if result.isSplit {
		defer node.unlock()
		split := node.insertSplit(result)
		if !result.isSplit {
			node.unlockParent(true)
		}
		return split
	}
	return Split{err: result.err}
	/* SOLUTION }}} */
}

// insertSplit inserts a split result into an internal node.
// If this insertion results in another split, the split is cascaded upwards.
func (node *InternalNode) insertSplit(split Split) Split {
	/* SOLUTION {{{ */
	insertPos := node.search(split.key)
	// Shift keys to the right.
	for i := node.numKeys - 1; i >= insertPos; i-- {
		node.updateKeyAt(i+1, node.getKeyAt(i))
	}
	// Shift children to the right.
	for i := node.numKeys; i > insertPos; i-- {
		node.updatePNAt(i+1, node.getPNAt(i))
	}
	// Insert the new key and pagenumber at this position.
	node.updateKeyAt(insertPos, split.key)
	node.updatePNAt(insertPos+1, split.rightPN)
	node.updateNumKeys(node.numKeys + 1)
	// Check if we need to split.
	if node.numKeys > KEYS_PER_INTERNAL_NODE {
		return node.split()
	}
	return Split{}
	/* SOLUTION }}} */
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *InternalNode) delete(key int64) {
	/* SOLUTION {{{ */
	node.unlockParent(true)
	// Get child.
	childIdx := node.search(key)
	child, err := node.getChildAt(childIdx, true)
	if err != nil {
		return
	}
	defer child.getPage().Put()
	node.initChild(child)
	// Delete from child.
	child.delete(key)
	/* SOLUTION }}} */
}

// split is a helper function that splits an internal node, then propagates the split upwards.
func (node *InternalNode) split() Split {
	/* SOLUTION {{{ */
	// Create a new internal node to split our keys.
	newNode, err := createInternalNode(node.page.GetPager())
	if err != nil {
		return Split{err: err}
	}
	defer newNode.getPage().Put()
	// Compute the midpoint based on the number of children to move.
	midpoint := (node.numKeys - 1) / 2
	// Transfer the keys to the new node.
	for i := midpoint; i <= node.numKeys; i++ {
		newNode.updatePNAt(newNode.numKeys, node.getPNAt(i))
		if i < node.numKeys {
			newNode.updateKeyAt(newNode.numKeys, node.getKeyAt(i))
			newNode.updateNumKeys(newNode.numKeys + 1)
		}
	}
	middleKey := node.getKeyAt(midpoint - 1)
	node.updateNumKeys(midpoint - 1)
	// Propagate the split.
	return Split{
		isSplit: true,
		key:     middleKey,
		leftPN:  node.page.GetPageNum(),
		rightPN: newNode.page.GetPageNum(),
	}
	/* SOLUTION }}} */
}

// get returns the value associated with a given key from the leaf node.
func (node *InternalNode) get(key int64) (value int64, found bool) {
	// [CONCURRENCY] Unlock parents.
	node.unlockParent(true)
	// Find the child.
	childIdx := node.search(key)
	child, err := node.getChildAt(childIdx, true)
	if err != nil {
		return 0, false
	}
	node.initChild(child)
	defer child.getPage().Put()
	return child.get(key)
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *InternalNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	index := node.search(key)
	child, err := node.getChildAt(index, false)
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
		child, err := node.getChildAt(idx, false)
		if err != nil {
			return
		}
		defer child.getPage().Put()
		child.printNode(w, nextFirstPrefix, nextPrefix)
		if idx != node.numKeys {
			io.WriteString(w, fmt.Sprintf("\n%v[KEY] %v\n", nextPrefix, node.getKeyAt(idx)))
		}
	}
}
