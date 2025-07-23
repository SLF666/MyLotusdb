package lotusdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/bwmarrin/snowflake"

	arenaskl "github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/rosedblabs/wal"
)

const (
	// wal文件名的格式是 .SEG.%d
	// %d is the unique id of the memtable, used to generate wal file name
	// for example, the wal file name of memtable with id 1 is .SEG.1.
	walFileExt     = ".SEG.%d"
	initialTableID = 1
)

type (
	// memtable 是一种内存中数据结构，用于存储在被写入 index 和 value log 之前的数据。
	// 目前仅支持跳表（skip list）数据结构
	//
	// 新增写入操作会将数据插入 memtable，读取操作则先从 memtable 查询数据，
	// 然后再从索引和值日志中读取，因为 memtable 中的数据更新
	//
	// 一旦 memtable 达到阈值（memtable 有其阈值，参见 options 中的 MemtableSize），
	// 它将变为不可变并被新的 memtable 替换
	//
	// 后台 goroutine 将 memtable 的内容写入index和 vlog，
	// 之后 memtable 可以被删除
	memtable struct {
		mu      sync.RWMutex
		wal     *wal.WAL           // write ahead log for the memtable
		skl     *arenaskl.Skiplist // in-memory skip list
		options memtableOptions
	}

	// memtableOptions： memtable的配置项
	memtableOptions struct {
		dirPath         string // where write ahead log wal file is stored
		tableID         uint32 // 通过一个唯一id将memtable与wal文件建立联系
		memSize         uint32 // max size of the memtable
		walBytesPerSync uint32 // 将WAL文件写入磁盘的吞吐量，以每同步字节数（BytesPerSync）为参数
		walSync         bool   // WAL 写入后立即刷新
	}
)

// 查找具有指定 ID 的 memtable 的 WAL 文件
// 每个 memtable 都会关联一个 WAL 文件，因此 WAL 文件名由 memtable 的 ID 生成的
// 例如，ID 为 1 的 memtable 的 WAL 文件名为 .SEG.1.
func openAllMemtables(options Options) ([]*memtable, error) {
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	// get all memtable ids
	var tableIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		var prefix int
		_, err = fmt.Sscanf(entry.Name(), "%d"+walFileExt, &prefix, &id)
		if err != nil {
			continue
		}
		tableIDs = append(tableIDs, id)
	}

	if len(tableIDs) == 0 {
		tableIDs = append(tableIDs, initialTableID)
	}
	sort.Ints(tableIDs)
	tables := make([]*memtable, len(tableIDs))
	for i, table := range tableIDs {
		table, errOpenMemtable := openMemtable(memtableOptions{
			dirPath:         options.DirPath,
			tableID:         uint32(table),
			memSize:         options.MemtableSize,
			walSync:         options.Sync,
			walBytesPerSync: options.BytesPerSync,
		})
		if errOpenMemtable != nil {
			return nil, errOpenMemtable
		}
		tables[i] = table
	}

	return tables, nil
}

// 从对应的wal中重新加载数据，并返回memtable
func openMemtable(options memtableOptions) (*memtable, error) {
	// init skip list
	//nolint:gomnd // default size
	skl := arenaskl.NewSkiplist(int64(float64(options.memSize) * 1.5))
	table := &memtable{options: options, skl: skl}

	// open the Write Ahead Log file
	walFile, err := wal.Open(wal.Options{
		DirPath:        options.dirPath,
		SegmentSize:    math.MaxInt, // 无限制，这个wal都是一个段
		SegmentFileExt: fmt.Sprintf(walFileExt, options.tableID),
		Sync:           options.walSync,
		BytesPerSync:   options.walBytesPerSync,
	})
	if err != nil {
		return nil, err
	}
	table.wal = walFile

	indexRecords := make(map[uint64][]*LogRecord)
	// now we get the opened wal file, we need to load all entries
	// from wal to rebuild the content of the skip list
	reader := table.wal.NewReader()
	for {
		chunk, _, errNext := reader.Next()
		if errNext != nil {
			if errors.Is(errNext, io.EOF) {
				break
			}
			return nil, errNext
		}
		record := decodeLogRecord(chunk)
		if record.Type == LogRecordBatchFinished {
			batchID, errParseBytes := snowflake.ParseBytes(record.Key)
			if errParseBytes != nil {
				return nil, errParseBytes
			}
			for _, idxRecord := range indexRecords[uint64(batchID)] {
				table.skl.Put(y.KeyWithTs(idxRecord.Key, 0),
					y.ValueStruct{Value: idxRecord.Value, Meta: idxRecord.Type})
			}
			delete(indexRecords, uint64(batchID))
		} else {
			indexRecords[record.BatchID] = append(indexRecords[record.BatchID], record)
		}
	}

	// open and read wal file successfully, return the memtable
	return table, nil
}

// 批量写入数据到wal 以及 memtable
func (mt *memtable) putBatch(pendingWrites map[string]*LogRecord,
	batchID snowflake.ID, options WriteOptions) error {
	// if wal is not disabled, write to wal first to ensure durability and atomicity
	if !options.DisableWal {
		// add record to wal.pendingWrites
		for _, record := range pendingWrites {
			record.BatchID = uint64(batchID)
			encRecord := encodeLogRecord(record)
			mt.wal.PendingWrites(encRecord)
		}

		// add a record to indicate the end of the batch
		endRecord := encodeLogRecord(&LogRecord{
			Key:  batchID.Bytes(),
			Type: LogRecordBatchFinished,
		})
		mt.wal.PendingWrites(endRecord)

		// write wal.pendingWrites
		if _, err := mt.wal.WriteAll(); err != nil {
			return err
		}
		// flush wal if necessary
		if options.Sync && !mt.options.walSync {
			if err := mt.wal.Sync(); err != nil {
				return err
			}
		}
	}

	mt.mu.Lock()
	// write to in-memory skip list
	for key, record := range pendingWrites {
		mt.skl.Put(y.KeyWithTs([]byte(key), 0), y.ValueStruct{Value: record.Value, Meta: record.Type})
	}
	mt.mu.Unlock()

	return nil
}

// 在内存表中获取val
// 如果这个key被标记为删除了，就返回true，否则返回false，[]byte是值或nil
func (mt *memtable) get(key []byte) (bool, []byte) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	valueStruct := mt.skl.Get(y.KeyWithTs(key, 0))
	deleted := valueStruct.Meta == LogRecordDeleted
	return deleted, valueStruct.Value
}

func (mt *memtable) isFull() bool {
	return mt.skl.MemSize() >= int64(mt.options.memSize)
}

func (mt *memtable) deleteWAl() error {
	if mt.wal != nil {
		return mt.wal.Delete()
	}
	return nil
}

func (mt *memtable) close() error {
	if mt.wal != nil {
		return mt.wal.Close()
	}
	return nil
}

func (mt *memtable) sync() error {
	if mt.wal != nil {
		return mt.wal.Sync()
	}
	return nil
}

// memtableIterator 是用来遍历内存中 memtable 的数据结构的，也就是个迭代器，因为我们知道跳表是用指针串着的
type memtableIterator struct {
	options IteratorOptions
	iter    *arenaskl.UniIterator
}

func newMemtableIterator(options IteratorOptions, memtable *memtable) *memtableIterator {
	return &memtableIterator{
		options: options,
		iter:    memtable.skl.NewUniIterator(options.Reverse),
	}
}

// 指向跳表的初始位置
func (mi *memtableIterator) Rewind() {
	mi.iter.Rewind()
	if len(mi.options.Prefix) == 0 {
		return
	}
	// prefix scan
	for mi.iter.Valid() && !bytes.HasPrefix(mi.iter.Key(), mi.options.Prefix) {
		mi.iter.Next()
	}
}

// Next moves the iterator to the next key.
func (mi *memtableIterator) Next() {
	mi.iter.Next()
	if len(mi.options.Prefix) == 0 {
		return
	}
	// prefix scan
	for mi.iter.Valid() && !bytes.HasPrefix(mi.iter.Key(), mi.options.Prefix) {
		mi.iter.Next()
	}
}

// Key get the current key.
func (mi *memtableIterator) Key() []byte {
	return y.ParseKey(mi.iter.Key())
}

// Value get the current value.
func (mi *memtableIterator) Value() any {
	return mi.iter.Value()
}

// Valid returns whether the iterator is exhausted.
func (mi *memtableIterator) Valid() bool {
	return mi.iter.Valid()
}

// Seek move the iterator to the key which is
// greater(less when reverse is true) than or equal to the specified key.
func (mi *memtableIterator) Seek(key []byte) {
	mi.iter.Seek(y.KeyWithTs(key, 0))
	if len(mi.options.Prefix) == 0 {
		return
	}
	// prefix scan
	for mi.Valid() && !bytes.HasPrefix(mi.Key(), mi.options.Prefix) {
		mi.Next()
	}
}

// Close the iterator.
func (mi *memtableIterator) Close() error {
	return mi.iter.Close()
}
