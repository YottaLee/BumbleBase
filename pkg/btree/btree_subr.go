package btree

import (
	"encoding/binary"

	pager "github.com/brown-csci1270/db/pkg/pager"
)

// We'll always maintain the invariant that the root's pagenum is 0.
// This saves us the effort of having to find the root node every time
// we open the database.
var ROOT_PN int64 = 0

// Node header constants.
var NODETYPE_OFFSET int64 = 0
var NODETYPE_SIZE int64 = 1
var NUM_KEYS_OFFSET int64 = NODETYPE_OFFSET + NODETYPE_SIZE
var NUM_KEYS_SIZE int64 = binary.MaxVarintLen64
var NODE_HEADER_SIZE int64 = NODETYPE_SIZE + NUM_KEYS_SIZE

// Leaf node header constants.
var RIGHT_SIBLING_PN_OFFSET int64 = NODE_HEADER_SIZE
var RIGHT_SIBLING_PN_SIZE int64 = binary.MaxVarintLen64
var LEAF_NODE_HEADER_SIZE int64 = NODE_HEADER_SIZE + RIGHT_SIBLING_PN_SIZE
var ENTRIES_PER_LEAF_NODE int64 = ((pager.PAGESIZE - LEAF_NODE_HEADER_SIZE) / ENTRYSIZE) - 1

// Internal node header constants.
var KEY_SIZE int64 = binary.MaxVarintLen64
var PN_SIZE int64 = binary.MaxVarintLen64
var INTERNAL_NODE_HEADER_SIZE int64 = NODE_HEADER_SIZE
var ptrSpace int64 = pager.PAGESIZE - INTERNAL_NODE_HEADER_SIZE - KEY_SIZE
var KEYS_PER_INTERNAL_NODE int64 = (ptrSpace / (KEY_SIZE + PN_SIZE)) - 1
var KEYS_OFFSET int64 = INTERNAL_NODE_HEADER_SIZE
var KEYS_SIZE int64 = KEY_SIZE * (KEYS_PER_INTERNAL_NODE + 1)
var PNS_OFFSET int64 = KEYS_OFFSET + KEYS_SIZE

// NodeType identifies if a node is a leaf node or internal node.
type NodeType bool

const (
	INTERNAL_NODE NodeType = false
	LEAF_NODE     NodeType = true
)

// NodeHeaders contain metadata common to all types of nodes
type NodeHeader struct {
	nodeType NodeType
	numKeys  int64
	page     *pager.Page
}

// Leaf Node definition
type LeafNode struct {
	NodeHeader           // Include header information
	rightSiblingPN int64 // Page number of the right sibling node
}

// Internal Node definition
type InternalNode struct {
	NodeHeader // Include header information
}

/////////////////////////////////////////////////////////////////////////////
//////////////////////// Generic Helper Functions ///////////////////////////
/////////////////////////////////////////////////////////////////////////////

// initPage resets the page then sets the nodeType variable.
func initPage(page *pager.Page, nodeType NodeType) {
	page.SetDirty(true)
	copy(*page.GetData(), make([]byte, pager.PAGESIZE))
	if nodeType == LEAF_NODE {
		(*page.GetData())[int(NODETYPE_OFFSET)] = 1 // Set the nodeType bit
	}
}

// pageToNode returns the node corresponding to the given page.
func pageToNode(page *pager.Page) Node {
	nodeHeader := pageToNodeHeader(page)
	if nodeHeader.nodeType == LEAF_NODE {
		return pageToLeafNode(page)
	}
	return pageToInternalNode(page)
}

// pageToNodeHeader returns node header data from the given page.
func pageToNodeHeader(page *pager.Page) NodeHeader {
	var nodeType NodeType
	if (*page.GetData())[NODETYPE_OFFSET] == 0 {
		nodeType = INTERNAL_NODE
	} else {
		nodeType = LEAF_NODE
	}
	numKeys, _ := binary.Varint(
		(*page.GetData())[NUM_KEYS_OFFSET : NUM_KEYS_OFFSET+NUM_KEYS_SIZE],
	)
	return NodeHeader{
		nodeType: nodeType,
		numKeys:  numKeys,
		page:     page,
	}
}

// cellPos computes the position of a cell within a page given a headersize.
func cellPos(headersize int64, cellnum int64) int64 {
	return headersize + cellnum*ENTRYSIZE
}

// keyPos returns the offset in the page to the internal node's ith key.
func keyPos(index int64) int64 {
	return KEYS_OFFSET + index*KEY_SIZE
}

// pnPos returns the page offset to the internal node's ith child's pagenumber
func pnPos(index int64) int64 {
	return PNS_OFFSET + index*PN_SIZE
}

/////////////////////////////////////////////////////////////////////////////
//////////////////// Leaf Node Subroutine Functions /////////////////////////
/////////////////////////////////////////////////////////////////////////////

// pageToLeafNode returns the leaf node at the corresponding page.
func pageToLeafNode(page *pager.Page) *LeafNode {
	nodeHeader := pageToNodeHeader(page)
	rightSiblingPN, _ := binary.Varint(
		(*page.GetData())[RIGHT_SIBLING_PN_OFFSET : RIGHT_SIBLING_PN_OFFSET+RIGHT_SIBLING_PN_SIZE],
	)
	return &LeafNode{
		nodeHeader,
		rightSiblingPN,
	}
}

// createLeafNode creates and returns a new leaf node.
// Nodes created with this function must be `Put()` accordingly after use.
func createLeafNode(pager *pager.Pager) (*LeafNode, error) {
	newPN := pager.GetFreePN()
	newPage, err := pager.GetPage(newPN)
	if err != nil {
		return &LeafNode{}, err
	}
	initPage(newPage, LEAF_NODE)
	return pageToLeafNode(newPage), nil
}

// getPage returns a pointer to the leaf node's page.
func (node *LeafNode) getPage() *pager.Page {
	return node.page
}

// getNodeType returns leafNode.
func (node *LeafNode) getNodeType() NodeType {
	return node.nodeType
}

// copy copies the attributes and data of toCopy to the leaf node.
func (node *LeafNode) copy(toCopy *LeafNode) {
	copy(*node.page.GetData(), *toCopy.page.GetData())
	node.updateNumKeys(toCopy.numKeys)
	node.setRightSibling(toCopy.rightSiblingPN)
}

// isRoot returns true if the current node is the root node.
func (node *LeafNode) isRoot() bool {
	return node.page.GetPageNum() == ROOT_PN
}

// setRightSibling sets the right sibling pagenumber attribute of the leaf node
// and updates the leaf node's page accordingly. returns the old right sibling.
func (node *LeafNode) setRightSibling(siblingPN int64) int64 {
	// Retrieve the old sibling data
	oldSiblingPN := node.rightSiblingPN
	// Write the new sibling data to the page
	node.rightSiblingPN = siblingPN
	siblingData := make([]byte, RIGHT_SIBLING_PN_SIZE)
	binary.PutVarint(siblingData, node.rightSiblingPN)
	node.page.Update(
		siblingData,
		RIGHT_SIBLING_PN_OFFSET,
		RIGHT_SIBLING_PN_SIZE,
	)
	return oldSiblingPN
}

// cellPos returns the page offset to the cell at the given index.
func (node *LeafNode) cellPos(index int64) int64 {
	return cellPos(LEAF_NODE_HEADER_SIZE, index)
}

// modifyCell updates the data stored in the cell at the given index.
func (node *LeafNode) modifyCell(index int64, entry BTreeEntry) {
	newdata := entry.Marshal()
	startPos := node.cellPos(index)
	node.page.Update(newdata, startPos, ENTRYSIZE)
}

// getCell returns the entry stored in the cell at the given index.
func (node *LeafNode) getCell(index int64) BTreeEntry {
	startPos := node.cellPos(index)
	// Deserialize the entry.
	entry := unmarshalEntry((*node.page.GetData())[startPos : startPos+ENTRYSIZE])
	return entry
}

// getKeyAt returns the key stored at the given index of the leaf node.
func (node *LeafNode) getKeyAt(index int64) int64 {
	return node.getCell(index).GetKey()
}

// updateKeyAt updates the key at the given index of the leaf node.
func (node *LeafNode) updateKeyAt(index int64, key int64) {
	entry := node.getCell(index)
	entry.SetKey(key)
	node.modifyCell(index, entry)
}

// getValueAt returns the value stored at the given index of the leaf node.
func (node *LeafNode) getValueAt(index int64) int64 {
	return node.getCell(index).GetValue()
}

// updateValueAt updates the value at the given index of the leaf node.
func (node *LeafNode) updateValueAt(index int64, value int64) {
	entry := node.getCell(index)
	entry.SetValue(value)
	node.modifyCell(index, entry)
}

// updateNumKeys updates the numKeys field in the node struct and the page.
func (node *LeafNode) updateNumKeys(nKeys int64) {
	node.numKeys = nKeys
	// Write the new data to the page
	nKeysData := make([]byte, NUM_KEYS_SIZE)
	binary.PutVarint(nKeysData, nKeys)
	node.page.Update(nKeysData, NUM_KEYS_OFFSET, NUM_KEYS_SIZE)
}

/////////////////////////////////////////////////////////////////////////////
///////////////// Internal Node Subroutine Functions ////////////////////////
/////////////////////////////////////////////////////////////////////////////

// pageToInternalNode returns the internal node corresponding to the given page.
func pageToInternalNode(page *pager.Page) *InternalNode {
	nodeHeader := pageToNodeHeader(page)
	return &InternalNode{nodeHeader}
}

// createInternalNode creates and returns a new internal node.
// Nodes created with this function must be `Put()` accordingly after use.
func createInternalNode(pager *pager.Pager) (*InternalNode, error) {
	newPN := pager.GetFreePN()
	newPage, err := pager.GetPage(newPN)
	if err != nil {
		return &InternalNode{}, err
	}
	initPage(newPage, INTERNAL_NODE)
	return pageToInternalNode(newPage), nil
}

// getPage returns the internal node's page.
func (node *InternalNode) getPage() *pager.Page {
	return node.page
}

// getNodeType returns internalNode.
func (node *InternalNode) getNodeType() NodeType {
	return node.nodeType
}

// copy copies the attributes and data of toCopy to node.
func (node *InternalNode) copy(toCopy *InternalNode) {
	copy(*node.page.GetData(), *toCopy.page.GetData())
	node.updateNumKeys(toCopy.numKeys)
}

// isRoot returns true if the current node is the root node.
func (node *InternalNode) isRoot() bool {
	return node.page.GetPageNum() == ROOT_PN
}

// getKeyAt returns the key stored at the given index of the internal node.
func (node *InternalNode) getKeyAt(index int64) int64 {
	startPos := keyPos(index)
	key, _ := binary.Varint((*node.page.GetData())[startPos : startPos+KEY_SIZE])
	return key
}

// updateKeyAt updates the key at the given index of the internal node.
func (node *InternalNode) updateKeyAt(index int64, key int64) {
	// Serialize the key data
	data := make([]byte, KEY_SIZE)
	binary.PutVarint(data, key)
	startPos := keyPos(int64(index))
	node.page.Update(data, startPos, KEY_SIZE)
}

// getPNAt returns the pagenumber stored at the given index of the internal node.
func (node *InternalNode) getPNAt(index int64) int64 {
	startPos := pnPos(index)
	pagenum, _ := binary.Varint((*node.page.GetData())[startPos : startPos+PN_SIZE])
	return pagenum
}

// updatePNAt updates the pagenumber at the given index of the internal node.
func (node *InternalNode) updatePNAt(index int64, pagenum int64) {
	// Serialize the pagenum data
	data := make([]byte, PN_SIZE)
	binary.PutVarint(data, pagenum)
	startPos := pnPos(int64(index))
	node.page.Update(data, startPos, PN_SIZE)
}

// getChildAt returns the internal node's ith child.
// Nodes created with this function must be `Put()` accordingly after use.
func (node *InternalNode) getChildAt(index int64) (Node, error) {
	// Get the child's page
	pagenum := node.getPNAt(index)
	page, err := node.page.GetPager().GetPage(pagenum)
	if err != nil {
		return &InternalNode{}, err
	}
	return pageToNode(page), nil
}

// updateNumKeys updates the numKeys field in the node struct and the page.
func (node *InternalNode) updateNumKeys(nKeys int64) {
	node.numKeys = nKeys
	// Write the new data to the page
	nKeysData := make([]byte, NUM_KEYS_SIZE)
	binary.PutVarint(nKeysData, nKeys)
	node.page.Update(nKeysData, NUM_KEYS_OFFSET, NUM_KEYS_SIZE)
}
