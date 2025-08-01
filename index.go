package lotusdb

import (
	"github.com/rosedblabs/diskhash"
)

const (
	// indexFileExt index文件的文件扩展名
	indexFileExt = "INDEX.%d"
)

// Index is the interface for index implementations.
// An index is a key-value store that maps keys to chunk positions.
// The index is used to find the chunk position of a key.
//
// Currently, the only implementation is a BoltDB index.
// But you can implement your own index if you want.
// index是个接口，可以使用不同的数据结构存储索引
type Index interface {
	// PutBatch put batch records to index
	PutBatch(keyPositions []*KeyPosition, matchKeyFunc ...diskhash.MatchKeyFunc) ([]*KeyPosition, error)

	// Get chunk position by key
	Get(key []byte, matchKeyFunc ...diskhash.MatchKeyFunc) (*KeyPosition, error)

	// DeleteBatch delete batch records from index
	DeleteBatch(keys [][]byte, matchKeyFunc ...diskhash.MatchKeyFunc) ([]*KeyPosition, error)

	// Sync sync index data to disk
	Sync() error

	// Close index
	Close() error
}

// open the specified index according to the index type
// currently, we support two index types: BTree and Hash,
// both of them are disk-based index.
func openIndex(options indexOptions) (Index, error) {
	switch options.indexType {
	case BTree:
		return openBTreeIndex(options)
	case Hash:
		return openHashIndex(options)
	default:
		panic("unknown index type")
	}
}

type IndexType int8

const (
	// BTree is the BoltDB index type.
	BTree IndexType = iota
	// Hash is the diskhash index type.
	// see: https://github.com/rosedblabs/diskhash
	Hash
)

type indexOptions struct {
	indexType IndexType

	dirPath string // index directory path

	partitionNum int // 分区的数量，也就是并发flush的数量

	keyHashFunction func([]byte) uint64 // hash function for sharding
}

// 通过hash函数返回属于哪个分区
func (io *indexOptions) getKeyPartition(key []byte) int {
	hashFn := io.keyHashFunction
	return int(hashFn(key) % uint64(io.partitionNum))
}
