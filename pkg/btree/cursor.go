package btree

import (
	"errors"

	utils "github.com/brown-csci1270/db/pkg/utils"
)

// Cursors are an abstration to represent locations in a table.
type BTreeCursor struct {
	table   *BTreeIndex // The table that this cursor point to.
	cellnum int64       // The cell number within a leaf node.
	isEnd   bool        // Indicates that this cursor points beyond the table/at the end of the table.
	curNode *LeafNode   // Current node.
}

// TableStart returns a cursor pointing to the first entry of the table.
func (table *BTreeIndex) TableStart() (utils.Cursor, error) {
	cursor := BTreeCursor{table: table, cellnum: 0}
	// Get the root page.
	curPage, err := table.pager.GetPage(table.rootPN)
	if err != nil {
		return nil, err
	}
	defer curPage.Put()
	curHeader := pageToNodeHeader(curPage)
	// Traverse the leftmost children until we reach a leaf node.
	for curHeader.nodeType != LEAF_NODE {
		curNode := pageToInternalNode(curPage)
		leftmostPN := curNode.getPNAt(0)
		curPage, err = table.pager.GetPage(leftmostPN)
		if err != nil {
			return nil, err
		}
		defer curPage.Put()
		curHeader = pageToNodeHeader(curPage)
	}
	// Set the cursor to point to the first entry in the leftmost leaf node.
	leftmostNode := pageToLeafNode(curPage)
	cursor.isEnd = (leftmostNode.numKeys == 0)
	cursor.curNode = leftmostNode
	return &cursor, nil
}

// TableEnd returns a cursor pointing to the last entry in the db.
// If the db is empty, returns a cursor to the new insertion position.
func (table *BTreeIndex) TableEnd() (utils.Cursor, error) {
	cursor := BTreeCursor{table: table, cellnum: 0}
	// Get the root page.
	curPage, err := table.pager.GetPage(table.rootPN)
	if err != nil {
		return &cursor, nil
	}
	defer curPage.Put()
	curHeader := pageToNodeHeader(curPage)
	// Traverse the leftmost children until we reach a leaf node.
	for curHeader.nodeType != LEAF_NODE {
		curNode := pageToInternalNode(curPage)
		rightMostPN := curNode.getPNAt(curNode.numKeys)
		curPage, err = table.pager.GetPage(rightMostPN)
		if err != nil {
			return nil, err
		}
		defer curPage.Put()
		curHeader = pageToNodeHeader(curPage)
	}
	// Set the cursor to point to the last entry in the rightmost leaf node.
	rightMostNode := pageToLeafNode(curPage)
	cursor.isEnd = (rightMostNode.numKeys == 0)
	cursor.curNode = rightMostNode
	cursor.cellnum = rightMostNode.numKeys - 1
	return &cursor, nil
}

// TableFind returns a cursor pointing to the given key.
// If the key is not found, returns a cursor to the new insertion position.
// Hint: use keyToNodeEntry
func (table *BTreeIndex) TableFind(key int64) (utils.Cursor, error) {
	cursor := BTreeCursor{table: table, cellnum: 0}
	// Get the root page.
	curPage, err := table.pager.GetPage(table.rootPN)
	if err != nil {
		return nil, err
	}
	defer curPage.Put()
	rootNode := pageToNode(curPage)
	cursorNode, idx, err := rootNode.keyToNodeEntry(key)

	if err != nil {
		cursor.cellnum = idx
		return &cursor, err
	}
	cursor.curNode = cursorNode
	cursor.cellnum = idx
	cursor.isEnd = cursorNode.numKeys == 0
	return &cursor, nil
}

// TableFindRange returns a slice of Entries with keys between the startKey and endKey.
func (table *BTreeIndex) TableFindRange(startKey int64, endKey int64) ([]utils.Entry, error) {
	ret := make([]utils.Entry, 0)

	// Get the cursor
	cursor, err := table.TableFind(startKey)
	if err != nil {
		return nil, err
	}

	entry, err := cursor.GetEntry()
	if err != nil {
		return nil, err
	}

	for !cursor.IsEnd() && entry.GetKey() < endKey {
		ret = append(ret, entry)
		err = cursor.StepForward()
		if err != nil {
			return nil, err
		}

		newEntry, err := cursor.GetEntry()
		if err != nil {
			return nil, err
		}
		entry = newEntry
	}

	return ret, nil
}

// stepForward moves the cursor ahead by one entry.
func (cursor *BTreeCursor) StepForward() error {
	// If the cursor is at the end of the node, try visiting the next node.
	if cursor.isEnd {
		// Get the next node's page number.
		nextPN := cursor.curNode.rightSiblingPN
		if nextPN < 0 {
			return errors.New("cannot advance the cursor further")
		}
		// Convert the page into a node.
		nextPage, err := cursor.table.pager.GetPage(nextPN)
		if err != nil {
			return err
		}
		defer nextPage.Put()
		nextNode := pageToLeafNode(nextPage)
		// Reinitialize the cursor.
		cursor.cellnum = 0
		cursor.isEnd = (cursor.cellnum == nextNode.numKeys)
		cursor.curNode = nextNode
		if cursor.isEnd {
			return cursor.StepForward()
		}
		return nil
	}
	// Else, just move forward one, potentially marking that we are at the end.
	cursor.cellnum++
	if cursor.cellnum >= cursor.curNode.numKeys {
		cursor.isEnd = true
	}
	return nil
}

// IsEnd returns true if at end.
func (cursor *BTreeCursor) IsEnd() bool {
	return cursor.isEnd
}

// getEntry returns the entry currently pointed to by the cursor.
func (cursor *BTreeCursor) GetEntry() (utils.Entry, error) {
	// Check if we're retrieving a non-existent entry.
	if cursor.isEnd {
		return BTreeEntry{}, errors.New("getEntry: entry is non-existent")
	}
	entry := cursor.curNode.getCell(cursor.cellnum)
	return entry, nil
}
