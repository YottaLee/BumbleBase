package hash

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	pager "github.com/brown-csci1270/db/pkg/pager"
	utils "github.com/brown-csci1270/db/pkg/utils"
)

// HashTable definitions.
type HashTable struct {
	depth   int64
	buckets []int64 // Array of bucket page numbers
	pager   *pager.Pager
	rwlock  sync.RWMutex // Lock on the hash table index
}

// Returns a new HashTable.
func NewHashTable(pager *pager.Pager) (*HashTable, error) {
	depth := int64(2)
	buckets := make([]int64, powInt(2, depth))
	for i := range buckets {
		bucket, err := NewHashBucket(pager, depth)
		if err != nil {
			return nil, err
		}
		buckets[i] = bucket.page.GetPageNum()
		bucket.page.Put()
	}
	return &HashTable{depth: depth, buckets: buckets, pager: pager}, nil
}

// [CONCURRENCY] Grab a write lock on the hash table index
func (table *HashTable) WLock() {
	table.rwlock.Lock()
}

// [CONCURRENCY] Release a write lock on the hash table index
func (table *HashTable) WUnlock() {
	table.rwlock.Unlock()
}

// [CONCURRENCY] Grab a read lock on the hash table index
func (table *HashTable) RLock() {
	table.rwlock.RLock()
}

// [CONCURRENCY] Release a read lock on the hash table index
func (table *HashTable) RUnlock() {
	table.rwlock.RUnlock()
}

// Get depth.
func (table *HashTable) GetDepth() int64 {
	return table.depth
}

// Get bucket page numbers.
func (table *HashTable) GetBuckets() []int64 {
	return table.buckets
}

// Get pager.
func (table *HashTable) GetPager() *pager.Pager {
	return table.pager
}

// Finds the entry with the given key.
func (table *HashTable) Find(key int64) (utils.Entry, error) {
	//panic("function not yet implemented");
	hash := Hasher(key, table.GetDepth())
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return nil, err
	}
	defer bucket.GetPage().Put()
	entry, found := bucket.Find(key)
	if found {
		return entry, nil
	} else {
		return entry, errors.New("can not found key")
	}
}

// ExtendTable increases the global depth of the table by 1.
func (table *HashTable) ExtendTable() {
	table.depth = table.depth + 1
	table.buckets = append(table.buckets, table.buckets...)
}

// Split the given bucket into two, extending the table if necessary.
func (table *HashTable) Split(bucket *HashBucket, hash int64) error {
	//panic("function not yet implemented")

	if bucket.depth == table.depth {
		// extend the table
		table.ExtendTable()
	}

	// calculate the hash key for the new bucket
	oldHash := Hasher(hash, bucket.depth)
	newHash := oldHash + 1<<bucket.depth

	newBucketDepth := bucket.depth + 1

	// increase the current bucket's depth by one
	bucket.updateDepth(newBucketDepth)

	// try to create the new bucket with depth increased by one
	newBucket, err := NewHashBucket(table.pager, newBucketDepth)
	if err != nil {
		return err
	}
	defer newBucket.GetPage().Put()

	// reorganize the entries in old bucket and move to new bucket if needed
	oldBucketEntryCount := int64(0)
	newBucketEntryCount := int64(0)

	// recalculate the hash for each key with the new depth and update the entries
	for i := int64(0); i < bucket.numKeys; i++ {
		key := bucket.getKeyAt(i)
		value := bucket.getValueAt(i)
		// calculate the hash of the key under the new depth
		hashedKey := Hasher(key, newBucketDepth)

		if hashedKey == oldHash {
			bucket.updateKeyAt(oldBucketEntryCount, key)
			bucket.updateValueAt(oldBucketEntryCount, value)
			oldBucketEntryCount += 1
		} else {
			newBucket.updateKeyAt(newBucketEntryCount, key)
			newBucket.updateValueAt(newBucketEntryCount, value)
			newBucketEntryCount += 1
		}
	}

	// update the numKeys after the reorganization
	bucket.updateNumKeys(oldBucketEntryCount)
	newBucket.updateNumKeys(newBucketEntryCount)

	newPN := newBucket.GetPage().GetPageNum()
	mask := (int64(1) << newBucketDepth) - 1
	for i := int64(0); i < powInt(2, table.depth); i++ {
		if (i & mask) == newHash {
			table.buckets[i] = newPN
		}
	}

	return nil
}

// Inserts the given key-value pair, splits if necessary.
func (table *HashTable) Insert(key int64, value int64) error {
	//panic("function not yet implemented")
	hash := Hasher(key, table.GetDepth())
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	defer bucket.GetPage().Put()
	// try to insert
	needSplit, err := bucket.Insert(key, value)
	if err != nil {
		return err
	}
	if !needSplit {
		return nil
	}
	// split bucket
	err = table.Split(bucket, hash)
	if err != nil {
		return err
	}
	// insert again
	hash = Hasher(key, table.GetDepth())
	newbucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	defer newbucket.GetPage().Put()
	newbucket.Insert(key, value)
	return nil
}

// Update the given key-value pair.
func (table *HashTable) Update(key int64, value int64) error {
	//panic("function not yet implemented")
	hash := Hasher(key, table.GetDepth())
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	defer bucket.GetPage().Put()
	return bucket.Update(key, value)
}

// Delete the given key-value pair, does not coalesce.
func (table *HashTable) Delete(key int64) error {
	//panic("function not yet implemented")
	hash := Hasher(key, table.GetDepth())
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	defer bucket.GetPage().Put()
	return bucket.Delete(key)
}

// Select all entries in this table.
func (table *HashTable) Select() ([]utils.Entry, error) {
	//panic("function not yet implemented")
	entrylist := make([]utils.Entry, 0)
	buckets := table.GetBuckets()
	for _, pn := range buckets {
		// Get bucket
		bucket, err := table.GetBucketByPN(pn)
		if err != nil {
			return entrylist, err
		}
		defer bucket.GetPage().Put()
		// Get all entries
		entries, err := bucket.Select()
		if err != nil {
			return nil, err
		}
		entrylist = append(entrylist, entries...)
	}
	return entrylist, nil
}

// Print out each bucket.
func (table *HashTable) Print(w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	io.WriteString(w, "====\n")
	io.WriteString(w, fmt.Sprintf("global depth: %d\n", table.depth))
	for i := range table.buckets {
		io.WriteString(w, fmt.Sprintf("====\nbucket %d\n", i))
		bucket, err := table.GetBucket(int64(i))
		if err != nil {
			continue
		}
		bucket.RLock()
		bucket.Print(w)
		bucket.RUnlock()
		bucket.page.Put()
	}
	io.WriteString(w, "====\n")
}

// Print out a specific bucket.
func (table *HashTable) PrintPN(pn int, w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	if int64(pn) >= table.pager.GetNumPages() {
		fmt.Println("out of bounds")
		return
	}
	bucket, err := table.GetBucketByPN(int64(pn))
	if err != nil {
		return
	}
	bucket.RLock()
	bucket.Print(w)
	bucket.RUnlock()
	bucket.page.Put()
}

// x^y
func powInt(x, y int64) int64 {
	return int64(math.Pow(float64(x), float64(y)))
}
