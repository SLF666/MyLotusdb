package lotusdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/gofrs/flock"
	"github.com/google/uuid"
	"github.com/rosedblabs/diskhash"
	"github.com/rosedblabs/wal"
	"golang.org/x/sync/errgroup"
)

const (
	fileLockName       = "FLOCK"
	deprecatedMetaName = "DEPMETA"
)

// DB 是 LotusDB 数据库的主要结构。
// 它包含运行数据库所需的所有信息。
//
// DB 是线程安全的，可以被多个 goroutine 使用。
// 但你不能同时使用相同的目录路径打开多个 DB。
// 如果这样做，将返回 ErrDatabaseIsUsing 错误。
//
// LotusDB 是用 Go 语言编写的最先进的键值数据库。
// 它结合了 LSM 树和 B+ 树的优势，读写速度都非常快。
// 它还非常节省内存，可以在单台机器上存储数十亿个键值对。

type DB struct {
	activeMem        *memtable            // 活跃的内存表，用于写入数据。
	immuMems         []*memtable          // 不可变的内存表，等待刷写到磁盘。
	index            Index                // 索引，存储键值及其位置的多分区索引。
	vlog             *valueLog            // 值日志，存储值数据的日志。
	fileLock         *flock.Flock         // 文件锁，用于防止多个进程使用同一数据库目录。
	flushChan        chan *memtable       // 刷新通道，用于通知刷新协程将内存表刷写到磁盘。
	flushLock        sync.Mutex           // 刷新锁，用于防止在合并操作未发生时进行刷新。
	compactChan      chan deprecatedState // 合并通道，用于通知分片需要进行合并操作。
	diskIO           *DiskIO              // 磁盘IO监控，用于监控磁盘的IO状态，并在适当时进行自动合并。
	mu               sync.RWMutex         // 用于保护数据库的并发访问。
	closed           bool                 // 数据库是否已关闭的标志。
	closeflushChan   chan struct{}        // 用于优雅地关闭刷新监听协程。
	closeCompactChan chan struct{}        // 用于优雅地关闭自动合并监听协程。
	options          Options              // 数据库的配置选项。
	batchPool        sync.Pool            // 批处理池，用于减少内存分配的开销。
}

// 使用指定的选项打开数据库。
// 如果数据库目录不存在，将自动创建。
//
// 多个进程不能同时使用同一个数据库目录，
// 否则将返回 ErrDatabaseIsUsing 错误。
//
// 首先打开 WAL 文件以重建内存表，然后打开索引和值日志。
// 如果成功，返回 DB 对象，否则返回错误。
func Open(options Options) (*DB, error) {
	// check whether all options are valid
	if err := validateOptions(&options); err != nil {
		return nil, err
	}

	// create data directory if not exist
	if _, err := os.Stat(options.DirPath); err != nil {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// create file lock, prevent multiple processes from using the same database directory
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// create deprecatedMeta file if not exist, read deprecatedNumber
	deprecatedMetaPath := filepath.Join(options.DirPath, deprecatedMetaName)
	deprecatedNumber, totalEntryNumber, err := loadDeprecatedEntryMeta(deprecatedMetaPath)
	if err != nil {
		return nil, err
	}

	// open all memtables
	memtables, err := openAllMemtables(options)
	if err != nil {
		return nil, err
	}

	// open index
	index, err := openIndex(indexOptions{
		indexType:       options.IndexType,
		dirPath:         options.DirPath,
		partitionNum:    options.PartitionNum,
		keyHashFunction: options.KeyHashFunction,
	})
	if err != nil {
		return nil, err
	}

	// open value log
	vlog, err := openValueLog(valueLogOptions{
		dirPath:               options.DirPath,
		segmentSize:           options.ValueLogFileSize,
		partitionNum:          uint32(options.PartitionNum),
		hashKeyFunction:       options.KeyHashFunction,
		compactBatchCapacity:  options.CompactBatchCapacity,
		deprecatedtableNumber: deprecatedNumber,
		totalNumber:           totalEntryNumber,
	})
	if err != nil {
		return nil, err
	}

	// init diskIO
	diskIO := new(DiskIO)
	diskIO.targetPath = options.DirPath
	diskIO.samplingInterval = options.DiskIOSamplingInterval
	diskIO.windowSize = options.DiskIOSamplingWindow
	diskIO.busyRate = options.DiskIOBusyRate
	diskIO.Init()

	db := &DB{
		activeMem:        memtables[len(memtables)-1],
		immuMems:         memtables[:len(memtables)-1],
		index:            index,
		vlog:             vlog,
		fileLock:         fileLock,
		flushChan:        make(chan *memtable, options.MemtableNums-1),
		closeflushChan:   make(chan struct{}),
		closeCompactChan: make(chan struct{}),
		compactChan:      make(chan deprecatedState),
		diskIO:           diskIO,
		options:          options,
		batchPool:        sync.Pool{New: makeBatch},
	}

	// if there are some immutable memtables when opening the database, flush them to disk
	if len(db.immuMems) > 0 {
		for _, table := range db.immuMems {
			db.flushMemtable(table)
		}
	}

	// start flush memtables goroutine asynchronously,
	// memtables with new coming writes will be flushed to disk if the active memtable is full.
	go db.listenMemtableFlush()

	if options.AutoCompactSupport {
		// 如果支持自动合并功能，启动自动合并的 Goroutine
		go db.listenAutoCompact()

		// 启动磁盘 I/O 状态监控
		// 如果启用了磁盘 I/O 监控，会根据磁盘的忙碌状态来判断是否进行合并操作
		if options.EnableDiskIO {
			go db.listenDiskIOState()
		}
	}

	return db, nil
}

// Close the database, close all data files and release file lock.
// Set the closed flag to true.
// The DB instance cannot be used after closing.
func (db *DB) Close() error {
	close(db.flushChan)
	<-db.closeflushChan // 会阻塞进程，直到db.flushChan关闭后会发送信息给它
	if db.options.AutoCompactSupport {
		close(db.compactChan)
		<-db.closeCompactChan
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// close all memtables
	for _, table := range db.immuMems {
		if err := table.close(); err != nil {
			return err
		}
	}
	if err := db.activeMem.close(); err != nil {
		return err
	}
	// close index
	if err := db.index.Close(); err != nil {
		return err
	}

	db.flushLock.Lock()
	// persist deprecated number and total entry number
	deprecatedMetaPath := filepath.Join(db.options.DirPath, deprecatedMetaName)
	err := storeDeprecatedEntryMeta(deprecatedMetaPath, db.vlog.deprecatedNumber, db.vlog.totalNumber)
	if err != nil {
		return err
	}
	defer db.flushLock.Unlock()

	// close value log
	if err = db.vlog.close(); err != nil {
		return err
	}
	// release file lock
	if err = db.fileLock.Unlock(); err != nil {
		return err
	}

	db.closed = true
	return nil
}

// Sync all data files to the underlying storage.
// 调用sync，刷入磁盘
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// sync all wal of memtables
	for _, table := range db.immuMems {
		if err := table.sync(); err != nil {
			return err
		}
	}
	if err := db.activeMem.sync(); err != nil {
		return err
	}
	// sync index
	if err := db.index.Sync(); err != nil {
		return err
	}
	// sync value log
	if err := db.vlog.sync(); err != nil {
		return err
	}

	return nil
}

// Put put with defaultWriteOptions.
func (db *DB) Put(key []byte, value []byte) error {
	return db.PutWithOptions(key, value, DefaultWriteOptions)
}

// PutWithOptions a key-value pair into the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Put operation.
func (db *DB) PutWithOptions(key []byte, value []byte, options WriteOptions) error {
	batch, ok := db.batchPool.Get().(*Batch)
	if !ok {
		panic("batchPoll.Get failed")
	}
	batch.options.WriteOptions = options
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single put operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, false, db).withPendingWrites()
	if err := batch.Put(key, value); err != nil {
		batch.unlock()
		return err
	}
	return batch.Commit()
}

// Get the value of the specified key from the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Get operation.
func (db *DB) Get(key []byte) ([]byte, error) {
	batch, ok := db.batchPool.Get().(*Batch)
	if !ok {
		panic("batchPoll.Get failed")
	}
	batch.init(true, false, true, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Get(key)
}

// Delete delete with defaultWriteOptions.
func (db *DB) Delete(key []byte) error {
	return db.DeleteWithOptions(key, DefaultWriteOptions)
}

// DeleteWithOptions the specified key from the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Delete operation.
func (db *DB) DeleteWithOptions(key []byte, options WriteOptions) error {
	batch, ok := db.batchPool.Get().(*Batch)
	if !ok {
		panic("batchPoll.Get failed")
	}
	batch.options.WriteOptions = options
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single delete operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, false, db).withPendingWrites()
	if err := batch.Delete(key); err != nil {
		batch.unlock()
		return err
	}
	return batch.Commit()
}

// Exist checks if the specified key exists in the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Exist operation.
func (db *DB) Exist(key []byte) (bool, error) {
	batch, ok := db.batchPool.Get().(*Batch)
	if !ok {
		panic("batchPoll.Get failed")
	}
	batch.init(true, false, true, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Exist(key)
}

// validateOptions validates the given options.
func validateOptions(options *Options) error {
	if options.DirPath == "" {
		return ErrDBDirectoryISEmpty
	}
	if options.MemtableSize <= 0 {
		options.MemtableSize = DefaultOptions.MemtableSize
	}
	if options.MemtableNums <= 0 {
		options.MemtableNums = DefaultOptions.MemtableNums
	}
	if options.PartitionNum <= 0 {
		options.PartitionNum = DefaultOptions.PartitionNum
	}
	if options.ValueLogFileSize <= 0 {
		options.ValueLogFileSize = DefaultOptions.ValueLogFileSize
	}
	// assure ValueLogFileSize >= MemtableSize
	if options.ValueLogFileSize < int64(options.MemtableSize) {
		options.ValueLogFileSize = int64(options.MemtableSize)
	}
	return nil
}

// get all memtables, including active memtable and immutable memtables.
// must be called with db.mu held.
func (db *DB) getMemTables() []*memtable {
	var tables []*memtable
	tables = append(tables, db.activeMem)

	last := len(db.immuMems) - 1
	for i := range db.immuMems {
		tables = append(tables, db.immuMems[last-i])
	}

	return tables
}

// waitMemtableSpace waits for space in the memtable.
// If the active memtable is full, it will be flushed to disk by the background goroutine.
// But if the flush speed is slower than the write speed, there may be no space in the memtable.
// So the write operation will wait for space in the memtable, and the timeout is specified by WaitMemSpaceTimeout.
func (db *DB) waitMemtableSpace() error {
	// 如果当前活跃的 Memtable 没有满，直接返回
	if !db.activeMem.isFull() {
		return nil
	}

	// 创建一个计时器，等待时间是 WaitMemSpaceTimeout
	timer := time.NewTimer(db.options.WaitMemSpaceTimeout)
	defer timer.Stop() // 在函数退出时停止计时器

	select {
	// 如果可以将当前活跃的 Memtable 放入 flush 通道，表示空间已释放
	case db.flushChan <- db.activeMem:
		// 将当前活跃的 Memtable 移到不可变 Memtables 列表
		db.immuMems = append(db.immuMems, db.activeMem)
		options := db.activeMem.options
		options.tableID++ // 增加 Memtable 的表 ID

		// 打开一个新的 Memtable 用于写入数据
		table, err := openMemtable(options)
		if err != nil {
			return err // 如果创建 Memtable 失败，返回错误
		}
		db.activeMem = table // 设置新的 Memtable 为当前活跃的 Memtable

	// 如果计时器超时，返回超时错误
	case <-timer.C:
		return ErrWaitMemtableSpaceTimeOut
	}

	return nil
}

// flushMemtable 将指定的内存表 flush 到磁盘。
// 执行以下步骤：
// 1. 遍历内存表中的所有记录，将其分为已删除键和日志记录。
// 2. 将日志记录写入值日志，获取键的位置。
// 3. 添加旧的 UUID，将所有键和位置写入索引。
// 4. 添加已删除的 UUID，并从索引中删除已删除的键。
// 5. 删除 WAL。
//
//nolint:funlen
func (db *DB) flushMemtable(table *memtable) {
	db.flushLock.Lock()
	defer db.flushLock.Unlock()

	sklIter := table.skl.NewIterator()
	var deletedKeys [][]byte
	var logRecords []*ValueLogRecord

	// iterate all records in memtable, divide them into deleted keys and log records
	// for every log record, we generate uuid.
	for sklIter.SeekToFirst(); sklIter.Valid(); sklIter.Next() {
		key, valueStruct := y.ParseKey(sklIter.Key()), sklIter.Value()
		if valueStruct.Meta == LogRecordDeleted {
			deletedKeys = append(deletedKeys, key)
		} else {
			logRecord := ValueLogRecord{key: key, value: valueStruct.Value, uid: uuid.New()}
			logRecords = append(logRecords, &logRecord)
		}
	}
	_ = sklIter.Close()
	// log.Println("len del:",len(deletedKeys),len(logRecords))

	// write to value log, get the positions of keys
	keyPos, err := db.vlog.writeBatch(logRecords)
	if err != nil {
		log.Println("vlog writeBatch failed:", err)
		return
	}

	// sync the value log
	if err = db.vlog.sync(); err != nil {
		log.Println("vlog sync failed:", err)
		return
	}

	// Add old key uuid into deprecatedtable, write all keys and positions to index.
	var putMatchKeys []diskhash.MatchKeyFunc
	if db.options.IndexType == Hash && len(keyPos) > 0 {
		putMatchKeys = make([]diskhash.MatchKeyFunc, len(keyPos))
		for i := range putMatchKeys {
			putMatchKeys[i] = MatchKeyFunc(db, keyPos[i].key, nil, nil)
		}
	}

	// Write all keys and positions to index.
	oldKeyPostions, err := db.index.PutBatch(keyPos, putMatchKeys...)
	if err != nil {
		log.Println("index PutBatch failed:", err)
		return
	}

	// Add old key uuid into deprecatedtable
	for _, oldKeyPostion := range oldKeyPostions {
		db.vlog.setDeprecated(oldKeyPostion.partition, oldKeyPostion.uid)
	}

	// Add deleted key uuid into deprecatedtable, and delete the deleted keys from index.
	var deleteMatchKeys []diskhash.MatchKeyFunc
	if db.options.IndexType == Hash && len(deletedKeys) > 0 {
		deleteMatchKeys = make([]diskhash.MatchKeyFunc, len(deletedKeys))
		for i := range deleteMatchKeys {
			deleteMatchKeys[i] = MatchKeyFunc(db, deletedKeys[i], nil, nil)
		}
	}

	// delete the deleted keys from index
	if oldKeyPostions, err = db.index.DeleteBatch(deletedKeys, deleteMatchKeys...); err != nil {
		log.Println("index DeleteBatch failed:", err)
		return
	}

	// uuid into deprecatedtable
	for _, oldKeyPostion := range oldKeyPostions {
		db.vlog.setDeprecated(oldKeyPostion.partition, oldKeyPostion.uid)
	}

	// sync the index
	if err = db.index.Sync(); err != nil {
		log.Println("index sync failed:", err)
		return
	}

	// delete the wal
	if err = table.deleteWAl(); err != nil {
		log.Println("delete wal failed:", err)
		return
	}

	// delete old memtable kept in memory
	db.mu.Lock()
	defer db.mu.Unlock()
	if table == db.activeMem {
		options := db.activeMem.options
		options.tableID++
		// open a new memtable for writing
		table, err = openMemtable(options)
		if err != nil {
			panic("flush activate memtable wrong")
		}
		db.activeMem = table
	} else {
		if len(db.immuMems) == 1 {
			db.immuMems = db.immuMems[:0]
		} else {
			db.immuMems = db.immuMems[1:]
		}
	}
	db.sendThresholdState()
}

func (db *DB) sendThresholdState() {
	if db.options.AutoCompactSupport {
		// 根据 vlog 中的总记录数和设定的压缩比例，计算出下限和上限阈值
		lowerThreshold := uint32((float32)(db.vlog.totalNumber) * db.options.AdvisedCompactionRate)
		upperThreshold := uint32((float32)(db.vlog.totalNumber) * db.options.ForceCompactionRate)

		// 默认的阈值状态为未达到压缩阈值
		thresholdState := deprecatedState{
			thresholdState: ThresholdState(UnarriveThreshold),
		}

		// 如果 deprecated 数据的数量达到或超过强制压缩的上限，设置为强制压缩状态
		if db.vlog.deprecatedNumber >= upperThreshold {
			thresholdState = deprecatedState{
				thresholdState: ThresholdState(ArriveForceThreshold),
			}
		} else if db.vlog.deprecatedNumber > lowerThreshold {
			// 如果 deprecated 数据的数量超过了建议的压缩下限，设置为建议压缩状态
			thresholdState = deprecatedState{
				thresholdState: ThresholdState(ArriveAdvisedThreshold),
			}
		}

		// 向 compactChan 通道发送当前的阈值状态，启动压缩操作
		select {
		case db.compactChan <- thresholdState:
		default: // 如果压缩操作正在进行中，直接跳过，不做任何操作
		}
	}
}

// 监听刷新信号
func (db *DB) listenMemtableFlush() {
	sig := make(chan os.Signal, 1)                                                                     // 创建一个信号通道，用于接收操作系统信号
	signal.Notify(sig, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT) // 监听几种中断信号（如 SIGINT、SIGTERM 等）

	for {
		select {
		case table, ok := <-db.flushChan: // 从 flushChan 通道接收内存表（memtable）数据
			if ok { // 如果通道打开且有数据
				db.flushMemtable(table) // 调用 flushMemtable 方法将内存表刷写到磁盘
			} else { // 如果通道关闭，意味着不再接收数据
				db.closeflushChan <- struct{}{} // 向 closeflushChan 通道发送信号，表示可以关闭刷新监听
				return                          // 退出 Goroutine
			}
		case <-sig: // 接收到中断信号（如用户终止程序的信号）
			return // 退出 Goroutine
		}
	}
}

// listenAutoComapct is an automated, more fine-grained approach that does not block Bptree.
// it dynamically detects the redundancy of each shard and decides
// determine whether to do compact based on the current IO state.
//
// 通过不同临界值，采用不同策略。当达到减一临界值，会查看磁盘状态再决定是否法索；当达到强制临界值，直接压缩
// 第一次压缩和非第一次压缩也会有所不同
//
//nolint:gocognit
func (db *DB) listenAutoCompact() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	firstCompact := true
	thresholdstate := ThresholdState(UnarriveThreshold)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case state, ok := <-db.compactChan:
			if ok {
				thresholdstate = state.thresholdState
			} else {
				db.closeCompactChan <- struct{}{}
				return
			}
		case <-sig:
			return
		case <-ticker.C:
			//nolint:nestif // It requires multiple nested conditions for different thresholds and error judgments.
			if thresholdstate == ThresholdState(ArriveForceThreshold) {
				var err error
				if firstCompact {
					firstCompact = false
					err = db.Compact()
				} else {
					err = db.CompactWithDeprecatedtable()
				}
				if err != nil {
					panic(err)
				}
				thresholdstate = ThresholdState(UnarriveThreshold)
			} else if thresholdstate == ThresholdState(ArriveAdvisedThreshold) {
				// determine whether to do compact based on the current IO state
				free := true
				var err error = nil
				if db.options.EnableDiskIO {
					free, err = db.diskIO.IsFree()
					if err != nil {
						panic(err)
					}
				}
				if free {
					if firstCompact {
						firstCompact = false
						err = db.Compact()
					} else {
						err = db.CompactWithDeprecatedtable()
					}
					if err != nil {
						panic(err)
					}
					thresholdstate = ThresholdState(UnarriveThreshold)
				} else {
					log.Println("IO Busy now")
				}
			}
		}
	}
}

func (db *DB) listenDiskIOState() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case <-sig:
			return
		default:
			err := db.diskIO.Monitor()
			if err != nil {
				panic(err)
			}
		}
	}
}

// Compact will iterate all values in vlog, and write the valid values to a new vlog file.
// Then replace the old vlog file with the new one, and delete the old one.
//
// 第一次压缩方法：遍历所有的vlog，存下它的有效数据，并替换旧的vlog（不同分区并发执行
//
//nolint:gocognit
func (db *DB) Compact() error {
	db.flushLock.Lock()
	defer db.flushLock.Unlock()

	log.Println("[Compact data]")
	openVlogFile := func(part int, ext string) *wal.WAL {
		walFile, err := wal.Open(wal.Options{
			DirPath:        db.vlog.options.dirPath,
			SegmentSize:    db.vlog.options.segmentSize,
			SegmentFileExt: fmt.Sprintf(ext, part),
			Sync:           false, // we will sync manually
			BytesPerSync:   0,     // the same as Sync
		})
		if err != nil {
			_ = walFile.Delete()
			panic(err)
		}
		return walFile
	}

	g, _ := errgroup.WithContext(context.Background())
	var capacity int64
	var capacityList = make([]int64, db.options.PartitionNum)
	for i := 0; i < int(db.vlog.options.partitionNum); i++ {
		part := i
		g.Go(func() error {
			newVlogFile := openVlogFile(part, tempValueLogFileExt)
			validRecords := make([]*ValueLogRecord, 0)
			reader := db.vlog.walFiles[part].NewReader()
			// iterate all records in wal, find the valid records
			for {
				chunk, pos, err := reader.Next()
				atomic.AddInt64(&capacity, int64(len(chunk)))
				capacityList[part] += int64(len(chunk))
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					_ = newVlogFile.Delete()
					return err
				}

				record := decodeValueLogRecord(chunk)
				var hashTableKeyPos *KeyPosition
				var matchKey func(diskhash.Slot) (bool, error)
				if db.options.IndexType == Hash {
					matchKey = MatchKeyFunc(db, record.key, &hashTableKeyPos, nil)
				}
				keyPos, err := db.index.Get(record.key, matchKey)
				if err != nil {
					_ = newVlogFile.Delete()
					return err
				}

				if db.options.IndexType == Hash {
					keyPos = hashTableKeyPos
				}

				if keyPos == nil {
					continue
				}
				if keyPos.partition == uint32(part) && reflect.DeepEqual(keyPos.position, pos) {
					validRecords = append(validRecords, record)
				}

				if capacity >= int64(db.vlog.options.compactBatchCapacity) {
					err = db.rewriteValidRecords(newVlogFile, validRecords, part)
					if err != nil {
						_ = newVlogFile.Delete()
						return err
					}
					validRecords = validRecords[:0]
					atomic.AddInt64(&capacity, -capacityList[part])
					capacityList[part] = 0
				}
			}

			if len(validRecords) > 0 {
				err := db.rewriteValidRecords(newVlogFile, validRecords, part)
				if err != nil {
					_ = newVlogFile.Delete()
					return err
				}
			}

			// replace the wal with the new one.
			_ = db.vlog.walFiles[part].Delete()
			_ = newVlogFile.Close()
			if err := newVlogFile.RenameFileExt(fmt.Sprintf(valueLogFileExt, part)); err != nil {
				return err
			}
			db.vlog.walFiles[part] = openVlogFile(part, valueLogFileExt)

			// clean dpTable after compact
			db.vlog.dpTables[part].clean()

			return nil
		})
	}
	db.vlog.cleanDeprecatedTable()
	return g.Wait()
}

// Compact will iterate all values in vlog, find old values by deprecatedtable,
// and write the valid values to a new vlog file.
// Then replace the old vlog file with the new one, and delete the old one.
//
// 非第一次，使用deprecatedtable来确定存在
//
//nolint:gocognit
func (db *DB) CompactWithDeprecatedtable() error {
	db.flushLock.Lock()
	defer db.flushLock.Unlock()

	log.Println("[CompactWithDeprecatedtable data]")
	openVlogFile := func(part int, ext string) *wal.WAL {
		walFile, err := wal.Open(wal.Options{
			DirPath:        db.vlog.options.dirPath,
			SegmentSize:    db.vlog.options.segmentSize,
			SegmentFileExt: fmt.Sprintf(ext, part),
			Sync:           false, // we will sync manually
			BytesPerSync:   0,     // the same as Sync
		})
		if err != nil {
			_ = walFile.Delete()
			panic(err)
		}
		return walFile
	}

	g, _ := errgroup.WithContext(context.Background())
	var capacity int64
	var capacityList = make([]int64, db.options.PartitionNum)
	for i := 0; i < int(db.vlog.options.partitionNum); i++ {
		part := i
		g.Go(func() error {
			newVlogFile := openVlogFile(part, tempValueLogFileExt)
			validRecords := make([]*ValueLogRecord, 0)
			reader := db.vlog.walFiles[part].NewReader()
			// iterate all records in wal, find the valid records
			for {
				chunk, pos, err := reader.Next()
				atomic.AddInt64(&capacity, int64(len(chunk)))
				capacityList[part] += int64(len(chunk))
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					_ = newVlogFile.Delete()
					return err
				}

				record := decodeValueLogRecord(chunk)
				if !db.vlog.isDeprecated(part, record.uid) {
					// not find old uuid in dptable, we add it to validRecords.
					validRecords = append(validRecords, record)
				}
				if db.options.IndexType == Hash {
					var hashTableKeyPos *KeyPosition
					// var matchKey func(diskhash.Slot) (bool, error)
					matchKey := MatchKeyFunc(db, record.key, &hashTableKeyPos, nil)
					var keyPos *KeyPosition
					keyPos, err = db.index.Get(record.key, matchKey)
					if err != nil {
						_ = newVlogFile.Delete()
						return err
					}

					if db.options.IndexType == Hash {
						keyPos = hashTableKeyPos
					}

					if keyPos == nil {
						continue
					}
					if keyPos.partition == uint32(part) && reflect.DeepEqual(keyPos.position, pos) {
						validRecords = append(validRecords, record)
					}
				}

				if capacity >= int64(db.vlog.options.compactBatchCapacity) {
					err = db.rewriteValidRecords(newVlogFile, validRecords, part)
					if err != nil {
						_ = newVlogFile.Delete()
						return err
					}
					validRecords = validRecords[:0]
					atomic.AddInt64(&capacity, -capacityList[part])
					capacityList[part] = 0
				}
			}
			if len(validRecords) > 0 {
				err := db.rewriteValidRecords(newVlogFile, validRecords, part)
				if err != nil {
					_ = newVlogFile.Delete()
					return err
				}
			}

			// replace the wal with the new one.
			_ = db.vlog.walFiles[part].Delete()
			_ = newVlogFile.Close()
			if err := newVlogFile.RenameFileExt(fmt.Sprintf(valueLogFileExt, part)); err != nil {
				return err
			}
			db.vlog.walFiles[part] = openVlogFile(part, valueLogFileExt)
			return nil
		})
	}

	err := g.Wait()
	db.vlog.cleanDeprecatedTable()
	return err
}

// 更新有效记录具体实现
func (db *DB) rewriteValidRecords(walFile *wal.WAL, validRecords []*ValueLogRecord, part int) error {
	for _, record := range validRecords {
		walFile.PendingWrites(encodeValueLogRecord(record))
	}

	walChunkPositions, err := walFile.WriteAll()
	if err != nil {
		return err
	}

	positions := make([]*KeyPosition, len(walChunkPositions))
	for i, walChunkPosition := range walChunkPositions {
		positions[i] = &KeyPosition{
			key:       validRecords[i].key,
			partition: uint32(part),
			position:  walChunkPosition,
		}
	}
	matchKeys := make([]diskhash.MatchKeyFunc, len(positions))
	if db.options.IndexType == Hash {
		for i := range matchKeys {
			matchKeys[i] = MatchKeyFunc(db, positions[i].key, nil, nil)
		}
	}
	_, err = db.index.PutBatch(positions, matchKeys...)
	return err
}

// load deprecated entries meta, and create meta file in first open.
//
// //nolint:nestif //default.
func loadDeprecatedEntryMeta(deprecatedMetaPath string) (uint32, uint32, error) {
	var err error
	var deprecatedNumber uint32
	var totalEntryNumber uint32
	if _, err = os.Stat(deprecatedMetaPath); os.IsNotExist(err) {
		// no exist, create one
		var file *os.File
		file, err = os.Create(deprecatedMetaPath)
		if err != nil {
			return deprecatedNumber, totalEntryNumber, err
		}
		deprecatedNumber = 0
		totalEntryNumber = 0
		file.Close()
	} else if err != nil {
		return deprecatedNumber, totalEntryNumber, err
	} else {
		// not err, we load meta
		var file *os.File
		file, err = os.Open(deprecatedMetaPath)
		if err != nil {
			return deprecatedNumber, totalEntryNumber, err
		}

		// set the file pointer to 0
		_, err = file.Seek(0, 0)
		if err != nil {
			return deprecatedNumber, totalEntryNumber, err
		}

		// read deprecatedNumber
		err = binary.Read(file, binary.LittleEndian, &deprecatedNumber)
		if err != nil {
			return deprecatedNumber, totalEntryNumber, err
		}

		// read totalEntryNumber
		err = binary.Read(file, binary.LittleEndian, &totalEntryNumber)
		if err != nil {
			return deprecatedNumber, totalEntryNumber, err
		}
	}
	return deprecatedNumber, totalEntryNumber, nil
}

// persist deprecated number and total entry number.
func storeDeprecatedEntryMeta(deprecatedMetaPath string, deprecatedNumber uint32, totalNumber uint32) error {
	file, err := os.OpenFile(deprecatedMetaPath, os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	// set the file pointer to 0 and overwrite
	_, err = file.Seek(0, 0)
	if err != nil {
		return err
	}

	// write deprecatedNumber
	err = binary.Write(file, binary.LittleEndian, &deprecatedNumber)
	if err != nil {
		return err
	}

	// write totalEntryNumber
	err = binary.Write(file, binary.LittleEndian, &totalNumber)
	if err != nil {
		return err
	}
	file.Close()
	return nil
}
