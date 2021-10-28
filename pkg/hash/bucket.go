package hash

import (
	"errors"
	"fmt"
	"io"

	pager "github.com/brown-csci1270/db/pkg/pager"
	utils "github.com/brown-csci1270/db/pkg/utils"
)

// HashBucket.
type HashBucket struct {
	depth   int64
	numKeys int64
	page    *pager.Page
}

// Construct a new HashBucket.
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

// Get local depth.
func (bucket *HashBucket) GetDepth() int64 {
	return bucket.depth
}

// Get a bucket's page.
func (bucket *HashBucket) GetPage() *pager.Page {
	return bucket.page
}

// Finds the entry with the given key.
func (bucket *HashBucket) Find(key int64) (utils.Entry, bool) {
	//panic("function not yet implemented");
	var idx int64
	for i := 0; i < int(bucket.numKeys); i++ {
		if bucket.getKeyAt(int64(i)) == key {
			idx = int64(i)
			return bucket.getCell(idx), true
		}
	}

	return nil, false
}

// Inserts the given key-value pair, splits if necessary.
func (bucket *HashBucket) Insert(key int64, value int64) (bool, error) {
	//panic("function not yet implemented")
	if bucket.numKeys == BUCKETSIZE {
		return true, nil
	}
	bucket.updateKeyAt(bucket.numKeys, key)
	bucket.updateValueAt(bucket.numKeys, value)
	bucket.updateNumKeys(bucket.numKeys + 1)
	return false, nil
}

// Update the given key-value pair, should never split.
func (bucket *HashBucket) Update(key int64, value int64) error {
	//panic("function not yet implemented");
	/*
		c := func(i int) bool {
			return bucket.getKeyAt(int64(i)) == key
		}
		i := int64(sort.Search(int(bucket.numKeys), c))
	*/
	var idx int64
	for i := 0; i < int(bucket.numKeys); i++ {
		if bucket.getKeyAt(int64(i)) == key {
			idx = int64(i)
			bucket.updateValueAt(idx, value)
			return nil
		}
	}

	return errors.New("Can not update nonexistent key value")

}

// Delete the given key-value pair, does not coalesce.
func (bucket *HashBucket) Delete(key int64) error {
	//panic("function not yet implemented");
	/*
		c := func(i int) bool {
			return bucket.getKeyAt(int64(i)) == key
		}
		idx := int64(sort.Search(int(bucket.numKeys), c))
	*/
	idx := int64(-1)
	for i := 0; i < int(bucket.numKeys); i++ {
		if bucket.getKeyAt(int64(i)) == key {
			idx = int64(i)
			break
		}
	}

	if idx == -1 {
		fmt.Printf("err, index: %d key: %d\n", idx, key)

		return errors.New("Can not delete nonexistent key")
	}

	for i := idx + 1; i < bucket.numKeys; i++ {
		bucket.updateKeyAt(i-1, bucket.getKeyAt(i))
		bucket.updateValueAt(i-1, bucket.getValueAt(i))

	}
	bucket.updateNumKeys(bucket.numKeys - 1)
	return nil
}

// Select all entries in this bucket.
func (bucket *HashBucket) Select() ([]utils.Entry, error) {
	//panic("function not yet implemented");
	entrylist := make([]utils.Entry, 0)
	if entrylist == nil {
		return nil, errors.New("select error: internal error")
	}
	for i := 0; i < int(bucket.numKeys); i++ {
		entry := bucket.getCell(int64(i))
		entrylist = append(entrylist, entry)
	}
	return entrylist, nil
}

// Pretty-print this bucket.
func (bucket *HashBucket) Print(w io.Writer) {
	io.WriteString(w, fmt.Sprintf("bucket depth: %d\n", bucket.depth))
	io.WriteString(w, "entries:")
	for i := int64(0); i < bucket.numKeys; i++ {
		bucket.getCell(i).Print(w)
	}
	io.WriteString(w, "\n")
}

// [CONCURRENCY] Grab a write lock on the hash table index
func (bucket *HashBucket) WLock() {
	bucket.page.WLock()
}

// [CONCURRENCY] Release a write lock on the hash table index
func (bucket *HashBucket) WUnlock() {
	bucket.page.WUnlock()
}

// [CONCURRENCY] Grab a read lock on the hash table index
func (bucket *HashBucket) RLock() {
	bucket.page.RLock()
}

// [CONCURRENCY] Release a read lock on the hash table index
func (bucket *HashBucket) RUnlock() {
	bucket.page.RUnlock()
}
