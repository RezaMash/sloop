package untyped

import (
	"bytes"
	"io"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v2"
	"github.com/salesforce/sloop/pkg/sloop/store/untyped/badgerwrap"
)

// MemFactory is a factory for creating in-memory DBs
type MemFactory struct{}

func (mf *MemFactory) Open(opt badger.Options) (badgerwrap.DB, error) {
	return &memDb{
		data: make(map[string][]byte),
	}, nil
}

// memDb is an in-memory implementation of the DB interface for testing
type memDb struct {
	sync.RWMutex
	data map[string][]byte
}

func (m *memDb) Close() error {
	return nil
}

func (m *memDb) Sync() error {
	return nil
}

func (m *memDb) Update(fn func(txn badgerwrap.Txn) error) error {
	m.Lock()
	defer m.Unlock()
	return fn(&memTxn{db: m, write: true})
}

func (m *memDb) View(fn func(txn badgerwrap.Txn) error) error {
	m.RLock()
	defer m.RUnlock()
	return fn(&memTxn{db: m, write: false})
}

func (m *memDb) DropPrefix(prefix []byte) error {
	m.Lock()
	defer m.Unlock()
	for k := range m.data {
		if bytes.HasPrefix([]byte(k), prefix) {
			delete(m.data, k)
		}
	}
	return nil
}

func (m *memDb) Size() (lsm, vlog int64) {
	m.RLock()
	defer m.RUnlock()
	var total int64
	for k, v := range m.data {
		total += int64(len(k) + len(v))
	}
	return total, 0
}

func (m *memDb) Tables(withKeysCount bool) []badger.TableInfo {
	return nil
}

func (m *memDb) Backup(w io.Writer, since uint64) (uint64, error) {
	return 0, nil
}

func (m *memDb) Flatten(workers int) error {
	return nil
}

func (m *memDb) Load(r io.Reader, maxPendingWrites int) error {
	return nil
}

func (m *memDb) RunValueLogGC(discardRatio float64) error {
	return nil
}

// memTxn is an in-memory implementation of the Txn interface
type memTxn struct {
	db    *memDb
	write bool
}

func (t *memTxn) Get(key []byte) (badgerwrap.Item, error) {
	val, ok := t.db.data[string(key)]
	if !ok {
		return nil, badger.ErrKeyNotFound
	}
	return &memItem{key: key, val: val}, nil
}

func (t *memTxn) Set(key, val []byte) error {
	if !t.write {
		return badger.ErrUpdateNeeded
	}
	t.db.data[string(key)] = val
	return nil
}

func (t *memTxn) Delete(key []byte) error {
	if !t.write {
		return badger.ErrUpdateNeeded
	}
	delete(t.db.data, string(key))
	return nil
}

func (t *memTxn) NewIterator(opt badger.IteratorOptions) badgerwrap.Iterator {
	keys := make([]string, 0, len(t.db.data))
	for k := range t.db.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return &memIterator{db: t.db, keys: keys, current: 0}
}

// memItem is an in-memory implementation of the Item interface
type memItem struct {
	key []byte
	val []byte
}

func (i *memItem) Key() []byte {
	return i.key
}

func (i *memItem) Value(fn func(val []byte) error) error {
	return fn(i.val)
}

func (i *memItem) ValueCopy(dst []byte) ([]byte, error) {
	return append(dst, i.val...), nil
}

func (i *memItem) EstimatedSize() int64 {
	return int64(len(i.key) + len(i.val))
}

func (i *memItem) IsDeletedOrExpired() bool {
	return false
}

func (i *memItem) KeyCopy(dst []byte) []byte {
	return append(dst, i.key...)
}

// memIterator is an in-memory implementation of the Iterator interface
type memIterator struct {
	db      *memDb
	keys    []string
	current int
}

func (it *memIterator) Close() {}

func (it *memIterator) Item() badgerwrap.Item {
	if it.current >= len(it.keys) {
		return nil
	}
	key := []byte(it.keys[it.current])
	val, _ := it.db.data[string(key)]
	return &memItem{key: key, val: val}
}

func (it *memIterator) Next() {
	it.current++
}

func (it *memIterator) Seek(key []byte) {
	it.current = sort.SearchStrings(it.keys, string(key))
}

func (it *memIterator) Valid() bool {
	return it.current < len(it.keys)
}

func (it *memIterator) ValidForPrefix(prefix []byte) bool {
	return it.Valid() && bytes.HasPrefix([]byte(it.keys[it.current]), prefix)
}

func (it *memIterator) Rewind() {
	it.current = 0
}
