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
	//panic("function not yet implemented");
	c := func(i int) bool {
		return node.getKeyAt(int64(i)) >= key
	}
	i := int64(sort.Search(int(node.numKeys), c))
	if i < node.numKeys {
		return i
	}
	return node.numKeys
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
// if update is true, allow overwriting existing keys. else, error.
func (node *LeafNode) insert(key int64, value int64, update bool) Split {
	//panic("function not yet implemented");
	//fmt.Printf("start to insert: key = %d , value = %d \n", key, value)

	index := node.search(key)
	ressplit := Split{
		isSplit: false,
		key:     -1,
		leftPN:  0,
		rightPN: 0,
		err:     nil,
	}
	// if update
	if update {
		// find the update key
		if index < node.numKeys && node.getKeyAt(index) == key {
			node.updateValueAt(index, value)
		} else {
			// can not find the update key
			ressplit.err = errors.New("Can not update nonexistent key")
			return ressplit
		}
	} else { // not update
		//insert key already exists
		if index < node.numKeys && node.getKeyAt(index) == key {
			ressplit.err = errors.New("Can not duplicate insert key")
			return ressplit
		} else { // insert the new key
			for i := node.numKeys - 1; i >= index; i-- {
				node.updateKeyAt(i+1, node.getKeyAt(i))
				node.updateValueAt(i+1, node.getValueAt(i))
			}
			node.updateKeyAt(index, key)
			node.updateValueAt(index, value)
			node.updateNumKeys(node.numKeys + 1)
		}
	}

	// have to insert new node
	//fmt.Printf("before insert, size : %d \n", node.numKeys)
	//fmt.Printf("after insert, size : %d \n", node.numKeys)

	// need to split
	if node.numKeys > ENTRIES_PER_LEAF_NODE {
		return node.split()
	}

	// need to update leftPN and rightPN?
	return ressplit
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *LeafNode) delete(key int64) {
	//panic("function not yet implemented");
	index := node.search(key)
	if index >= node.numKeys || node.getKeyAt(index) != key {
		return
	}
	for i := index; i < node.numKeys-1; i++ {
		node.updateKeyAt(i, node.getKeyAt(i+1))
		node.updateValueAt(i, node.getValueAt(i+1))
	}
	node.updateNumKeys(node.numKeys - 1)
}

// split is a helper function to split a leaf node, then propagate the split upwards.
func (node *LeafNode) split() Split {
	//panic("function not yet implemented");
	result := Split{
		isSplit: true,
		key:     -1,
		leftPN:  -1,
		rightPN: -1,
		err:     nil,
	}
	nextNode, err := createLeafNode(node.page.GetPager())
	if err != nil {
		result.err = err
		return result
	}
	defer nextNode.getPage().Put()

	// copy data
	startIndex := node.numKeys / 2
	newNumKeys := node.numKeys - startIndex
	nextNode.updateNumKeys(newNumKeys)
	for i := startIndex; i < node.numKeys; i++ {
		nextNode.updateKeyAt(i-startIndex, node.getKeyAt(i))
		nextNode.updateValueAt(i-startIndex, node.getValueAt(i))
	}
	// set pointers
	nextNode.setRightSibling(node.rightSiblingPN)
	node.updateNumKeys(startIndex)
	node.setRightSibling(nextNode.getPage().GetPageNum())

	result.key = nextNode.getKeyAt(0)
	result.leftPN = node.getPage().GetPageNum()
	result.rightPN = nextNode.getPage().GetPageNum()

	return result
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
/*
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
*/

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
	//panic("function not yet implemented");
	c := func(i int) bool {
		return node.getKeyAt(int64(i)) > key
	}
	i := int64(sort.Search(int(node.numKeys), c))
	return i
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
func (node *InternalNode) insert(key int64, value int64, update bool) Split {
	//panic("function not yet implemented");
	//search index, recursion in child node
	i := node.search(key)
	childnode, err := node.getChildAt(i)
	if err != nil {
		return Split{false, 0, 0, 0, err}
	}
	defer childnode.getPage().Put()

	split_struct := childnode.insert(key, value, update)
	// if child node need split
	if split_struct.isSplit {
		//insert split into internal node
		internal_split := node.insertSplit(split_struct)
		return internal_split
	}
	return split_struct
}

// insertSplit inserts a split result into an internal node.
// If this insertion results in another split, the split is cascaded upwards.
func (node *InternalNode) insertSplit(split Split) Split {
	//panic("function not yet implemented");
	result := Split{
		isSplit: false,
		key:     -1,
		leftPN:  -1,
		rightPN: -1,
		err:     split.err,
	}
	if !split.isSplit {
		return result
	}
	// find index for split
	index := node.search(split.key)
	// shift index behind, and insert new key
	node.updatePNAt(node.numKeys+1, node.getPNAt(node.numKeys))
	for i := node.numKeys - 1; i > index; i-- {
		node.updateKeyAt(i+1, node.getKeyAt(i))
		node.updatePNAt(i+1, node.getPNAt(i))
	}
	node.updateNumKeys(node.numKeys + 1)
	node.updateKeyAt(index, split.key)
	node.updatePNAt(index, split.leftPN)
	node.updatePNAt(index+1, split.rightPN)

	// if need split again
	if node.numKeys > KEYS_PER_INTERNAL_NODE {
		new_split := node.split()
		return new_split
	}
	return result
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *InternalNode) delete(key int64) {
	//panic("function not yet implemented");
	//search index, recursion in child node
	i := node.search(key)
	childnode, err := node.getChildAt(i)
	if err != nil {
		return
	}
	defer childnode.getPage().Put()
	childnode.delete(key)
}

// split is a helper function that splits an internal node, then propagates the split upwards.
func (node *InternalNode) split() Split {
	//panic("function not yet implemented");
	result := Split{
		isSplit: true,
		key:     -1,
		leftPN:  -1,
		rightPN: -1,
		err:     nil,
	}
	if node.numKeys < ENTRIES_PER_LEAF_NODE {
		result.isSplit = false
		return result
	}
	nextNode, err := createInternalNode(node.page.GetPager())
	defer nextNode.getPage().Put()
	if err != nil {
		result.err = err
		return result
	}
	// key index and new node key size
	startIndex := node.numKeys / 2
	newNumKeys := node.numKeys - startIndex - 1
	result.key = node.getKeyAt(startIndex)

	//copying data
	for i := startIndex + 1; i < node.numKeys; i++ {
		nextNode.updateKeyAt(i-startIndex-1, node.getKeyAt(i))
		nextNode.updatePNAt(i-startIndex-1, node.getPNAt(i))
	}
	nextNode.updateKeyAt(newNumKeys, node.getKeyAt(node.numKeys))
	nextNode.updateNumKeys(newNumKeys)

	// set original node size
	node.updateNumKeys(startIndex)

	result.leftPN = node.getPage().GetPageNum()
	result.rightPN = nextNode.getPage().GetPageNum()

	return result
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
/*
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
		if idx != node.numKeys {
			io.WriteString(w, fmt.Sprintf("\n%v[KEY] %v\n", nextPrefix, node.getKeyAt(idx)))
		}
	}
}
*/

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
