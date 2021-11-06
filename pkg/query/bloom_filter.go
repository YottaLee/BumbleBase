package query

import (
	bitset "github.com/bits-and-blooms/bitset"
	hash "github.com/brown-csci1270/db/pkg/hash"
)

type BloomFilter struct {
	size int64
	bits *bitset.BitSet
}

// CreateFilter initializes a BloomFilter with the given size.
func CreateFilter(size int64) *BloomFilter {
	//panic("function not yet implemented");
	bits := bitset.New(uint(size))
	return &BloomFilter{size: size, bits: bits}
}

// Insert adds an element into the bloom filter.
func (filter *BloomFilter) Insert(key int64) {
	//panic("function not yet implemented");
	xxHash := hash.XxHasher(key, filter.size)
	murmurHash := hash.MurmurHasher(key, filter.size)

	xxHash %= uint(filter.size)
	murmurHash %= uint(filter.size)

	filter.bits.Set(xxHash)
	filter.bits.Set(murmurHash)
}

// Contains checks if the given key can be found in the bloom filter/
func (filter *BloomFilter) Contains(key int64) bool {
	//panic("function not yet implemented")
	xxHash := hash.XxHasher(key, filter.size)
	murmurHash := hash.MurmurHasher(key, filter.size)

	xxHash %= uint(filter.size)
	murmurHash %= uint(filter.size)

	return filter.bits.Test(xxHash) && filter.bits.Test(murmurHash)
}
