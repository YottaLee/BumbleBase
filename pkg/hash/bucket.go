package hash

import (
	"errors"
	"fmt"
	"io"

	pager "github.com/brown-csci1270/db/pkg/pager"
	utils "github.com/brown-csci1270/db/pkg/utils"
)

// HashBucket HashBucket.
type HashBucket struct {
	depth   int64
	numKeys int64
	page    *pager.Page
}

// NewHashBucket Construct a new HashBucket.
func NewHashBucket(pager *pager.Pager, depth int64) (*HashBucket, error) {
	newPN := pager.GetFreePN()
	newPage, err := pager.GetPage(newPN)
	if err != nil {
		return nil, err
	}
	bucket := &HashBucket{depth: depth, numKeys: 0, page: newPage}
	bucket.updateDepth(depth)
	return bucket, nil
}

// GetDepth Get local depth.
func (bucket *HashBucket) GetDepth() int64 {
	return bucket.depth
}

// GetPage Get a bucket's page.
func (bucket *HashBucket) GetPage() *pager.Page {
	return bucket.page
}

// Find the entry with the given key.
func (bucket *HashBucket) Find(key int64) (utils.Entry, bool) {
	for i := int64(0); i < bucket.numKeys; i++ {
		if bucket.getKeyAt(i) == key {
			return bucket.getCell(i), true
		}
	}
	return nil, false
}

// Insert the given key-value pair, splits if necessary.
func (bucket *HashBucket) Insert(key int64, value int64) (bool, error) {
	bucket.modifyCell(bucket.numKeys, HashEntry{key: key, value: value})
	bucket.updateNumKeys(bucket.numKeys + 1)
	return bucket.numKeys >= BUCKETSIZE, nil
}

// Update the given key-value pair, should never split.
func (bucket *HashBucket) Update(key int64, value int64) error {
	// search for the entry to update
	for i := int64(0); i < bucket.numKeys; i++ {
		if bucket.getKeyAt(i) == key {
			bucket.updateValueAt(i, value)
			return nil
		}
	}
	// if not found, return key not found error
	return errors.New("update error: key not found")
}

// Delete the given key-value pair, does not coalesce.
func (bucket *HashBucket) Delete(key int64) error {
	deletionIdx := int64(-1)
	// search for the entry to delete
	for i := int64(0); i < bucket.numKeys; i++ {
		if bucket.getKeyAt(i) == key {
			deletionIdx = i
			break
		}
	}

	// if not found, return key not found error
	if deletionIdx == -1 {
		return errors.New("delete error: key not found")
	}

	// if found, shift all the key from deletionIdx + 1 to numKeys - 1 to left by one
	for i := deletionIdx + 1; i < bucket.numKeys; i++ {
		bucket.updateKeyAt(i - 1, bucket.getKeyAt(i))
		bucket.updateValueAt(i - 1, bucket.getValueAt(i));
	}

	// decrease numKeys by one
	bucket.updateNumKeys(bucket.numKeys - 1)
	return nil
}

// Select all entries in this bucket.
func (bucket *HashBucket) Select() ([]utils.Entry, error) {
	ret := make([]utils.Entry, 0)

	if ret == nil {
		return nil, errors.New("select error: internal error")
	}

	// append all the entries
	for i := int64(0); i < bucket.numKeys; i++ {
		ret = append(ret, bucket.getCell(i))
	}
	return ret, nil
}

// Print Pretty-print this bucket.
func (bucket *HashBucket) Print(w io.Writer) {
	io.WriteString(w, fmt.Sprintf("bucket depth: %d\n", bucket.depth))
	io.WriteString(w, "entries:")
	for i := int64(0); i < bucket.numKeys; i++ {
		bucket.getCell(i).Print(w)
	}
	io.WriteString(w, "\n")
}

// WLock [CONCURRENCY] Grab a write lock on the hash table index
func (bucket *HashBucket) WLock() {
	bucket.page.WLock()
}

// WUnlock [CONCURRENCY] Release a write lock on the hash table index
func (bucket *HashBucket) WUnlock() {
	bucket.page.WUnlock()
}

// RLock [CONCURRENCY] Grab a read lock on the hash table index
func (bucket *HashBucket) RLock() {
	bucket.page.RLock()
}

// RUnlock [CONCURRENCY] Release a read lock on the hash table index
func (bucket *HashBucket) RUnlock() {
	bucket.page.RUnlock()
}
