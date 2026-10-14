package db

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/talaria0101/moraine/internal/key"
	"github.com/talaria0101/moraine/internal/manifest"
	"github.com/talaria0101/moraine/internal/mem"
	"github.com/talaria0101/moraine/internal/table"
	"github.com/talaria0101/moraine/internal/wal"
)

// ErrClosed is returned by operations after Close.
var ErrClosed = errors.New("moraine: database is closed")

// Stats is a point-in-time copy of the engine counters.
type Stats struct {
	WALBytes        uint64
	SSTBytesWritten uint64
	Flushes         uint64
	Compactions     uint64
}

type flushJob struct {
	t      *mem.Table
	walID  uint64
	maxSeq uint64 // highest sequence in t; set once the flush publishes
}

type readerRef struct {
	r    *table.Reader
	refs int
	dead bool // input to a finished compaction; close when refs hit 0
}

// DB is a single-process LSM-tree KV store rooted at a directory.
type DB struct {
	opts Options
	dir  string
	man  *manifest.Manifest

	seq      atomic.Uint64 // last assigned sequence number
	nextFile atomic.Uint64

	mu      sync.Mutex
	cond    *sync.Cond // signals: flush work, stall relief, closing
	mem     *mem.Table
	wal     *wal.Writer
	walID   uint64
	imm     []flushJob // oldest first
	closing bool

	closingFlag atomic.Bool // lock-free view for the background loops

	readersMu sync.RWMutex
	readers   map[uint64]*readerRef

	stallMu   sync.Mutex
	stallCond *sync.Cond // wakes the compactor when new work appears

	closeOnce sync.Once
	closeErr  error
	bgWG      sync.WaitGroup
	bgErr     atomic.Pointer[bgError]

	// flushedTo holds the highest sequence whose flush has been
	// published in the manifest. Observers that need "everything
	// written so far is on disk" wait for this, not for the flush
	// queue to empty: the queue entry is popped before the flush runs.
	flushedTo atomic.Uint64

	stats struct {
		walBytes    atomic.Uint64
		sstWritten  atomic.Uint64
		flushes     atomic.Uint64
		compactions atomic.Uint64
	}
}

type bgError struct{ err error }

// Open recovers and opens the database in dir, creating it when
// missing. Recovery replays every WAL in file-number order, flushes
// the replayed entries into a new L0 table, then drops the WALs, so
// the memtable is empty and every recovered write is durable when
// Open returns. Table files no longer named by the manifest are
// removed as orphans.
func Open(dir string, opts Options) (*DB, error) {
	opts.fill()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	man, err := manifest.Open(filepath.Join(dir, "MANIFEST"))
	if err != nil {
		return nil, err
	}
	db := &DB{
		opts:    opts,
		dir:     dir,
		man:     man,
		readers: map[uint64]*readerRef{},
	}
	db.cond = sync.NewCond(&db.mu)
	db.stallCond = sync.NewCond(&db.stallMu)
	db.nextFile.Store(man.NextFileID())

	// Replay every WAL in ascending file order; later files carry
	// later writes and win through higher assigned sequences.
	replayMem := mem.New()
	logs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	sort.Slice(logs, func(a, b int) bool { return logID(logs[a]) < logID(logs[b]) })
	base := man.RecoveredNextSeq() // next sequence to hand out
	if base == 0 {
		base = 1
	}
	var replayed uint64
	for _, path := range logs {
		n, err := replayWAL(path, replayMem, base+replayed)
		if err != nil {
			man.Close()
			return nil, err
		}
		replayed += n
	}
	db.seq.Store(base + replayed - 1)

	// Persist recovered entries so they never depend on the WALs
	// again, then delete the WALs.
	if replayMem.Len() > 0 {
		if err := db.flushMemtable(replayMem, db.seq.Load()); err != nil {
			man.Close()
			return nil, err
		}
	}
	for _, path := range logs {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			man.Close()
			return nil, err
		}
	}

	if err := db.sweepOrphans(); err != nil {
		man.Close()
		return nil, err
	}

	// Fresh WAL for the active memtable.
	walID := db.allocFileID()
	w, err := wal.Open(db.logPath(walID))
	if err != nil {
		man.Close()
		return nil, err
	}
	db.wal = w
	db.walID = walID
	db.mem = mem.New()

	if err := syncDir(dir); err != nil {
		db.man.Close()
		return nil, err
	}

	db.bgWG.Add(2)
	go db.flushLoop()
	go db.compactLoop()
	return db, nil
}

// replayWAL applies every intact batch of the WAL at path into t,
// starting at sequence base, and returns how many entries it applied.
func replayWAL(path string, t *mem.Table, base uint64) (uint64, error) {
	r, err := wal.OpenReader(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer r.Close()
	var n uint64
	for {
		b, err := r.Next()
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			// Torn or corrupt tail: everything before it stands.
			return n, nil
		}
		for _, e := range b.Entries {
			t.Put(key.Encode(nil, e.Key, base+n, e.Type), e.Value)
			n++
		}
	}
}

// sweepOrphans deletes table files the manifest does not name, left
// behind by a crash between file creation and the manifest apply.
func (db *DB) sweepOrphans() error {
	files, _ := filepath.Glob(filepath.Join(db.dir, "*.sst"))
	live := map[uint64]bool{}
	for _, id := range db.man.Current().FileIDs() {
		live[id] = true
	}
	for _, f := range files {
		if !live[tableID(f)] {
			if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	// Half-written snapshot rewrites.
	tmp := filepath.Join(db.dir, "MANIFEST.tmp")
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (db *DB) logPath(id uint64) string {
	return filepath.Join(db.dir, strconv.FormatUint(id, 10)+".log")
}

func (db *DB) sstPath(id uint64) string {
	return filepath.Join(db.dir, strconv.FormatUint(id, 10)+".sst")
}

func logID(path string) uint64 {
	base := filepath.Base(path)
	id, _ := strconv.ParseUint(base[:len(base)-len(".log")], 10, 64)
	return id
}

func tableID(path string) uint64 {
	base := filepath.Base(path)
	id, _ := strconv.ParseUint(base[:len(base)-len(".sst")], 10, 64)
	return id
}

func (db *DB) allocFileID() uint64 {
	return db.nextFile.Add(1)
}

// Stats returns a snapshot of the counters.
func (db *DB) Stats() Stats {
	return Stats{
		WALBytes:        db.stats.walBytes.Load(),
		SSTBytesWritten: db.stats.sstWritten.Load(),
		Flushes:         db.stats.flushes.Load(),
		Compactions:     db.stats.compactions.Load(),
	}
}

// bgFailure returns the first background error, if any. Flush and
// compaction failures surface here on the next operation.
func (db *DB) bgFailure() error {
	if p := db.bgErr.Load(); p != nil {
		return p.err
	}
	return nil
}

func (db *DB) setBgError(err error) {
	db.bgErr.CompareAndSwap(nil, &bgError{err: err})
}

// Put stores value under userKey.
func (db *DB) Put(userKey, value []byte) error {
	return db.write([]wal.Entry{{Key: userKey, Value: value, Type: key.TypeSet}})
}

// Delete removes userKey; reads after Delete report not-found even if
// older versions exist in tables.
func (db *DB) Delete(userKey []byte) error {
	return db.write([]wal.Entry{{Key: userKey, Type: key.TypeDelete}})
}

func batchBytes(entries []wal.Entry) int {
	n := 8
	for _, e := range entries {
		n += 2 + len(e.Key) + 2 + len(e.Value) + 10 // keys, values, uvarint slack
	}
	return n
}

// write assigns sequences, appends the batch to the WAL, and inserts
// into the active memtable. Rotate first when the memtable is full.
func (db *DB) write(entries []wal.Entry) error {
	if err := db.bgFailure(); err != nil {
		return err
	}
	est := batchBytes(entries)
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closing {
		return ErrClosed
	}
	// Write stall: too many memtables waiting to be flushed. The
	// flusher broadcasts after each completed flush.
	for len(db.imm) >= 2 && !db.closing {
		db.cond.Wait()
	}
	if db.closing {
		return ErrClosed
	}
	if db.mem.Size()+int64(est) > int64(db.opts.WriteBuffer) {
		if err := db.rotateLocked(); err != nil {
			return err
		}
	}
	base := db.seq.Add(uint64(len(entries))) - uint64(len(entries)) + 1
	b := &wal.Batch{Entries: entries, Seq: base}
	if err := db.wal.Append(b, db.opts.Sync); err != nil {
		// The sequences were never made visible; hand them back.
		db.seq.Add(-uint64(len(entries)))
		return err
	}
	db.stats.walBytes.Add(uint64(est))
	for i := range entries {
		db.mem.Put(key.Encode(nil, entries[i].Key, base+uint64(i), entries[i].Type), entries[i].Value)
	}
	return nil
}

// rotateLocked freezes the active memtable for flushing and installs
// a fresh memtable and WAL. Callers hold db.mu.
func (db *DB) rotateLocked() error {
	oldWal := db.wal
	oldMem := db.mem
	oldWalID := db.walID
	walID := db.allocFileID()
	w, err := wal.Open(db.logPath(walID))
	if err != nil {
		return err
	}
	db.wal = w
	db.walID = walID
	db.mem = mem.New()
	db.imm = append(db.imm, flushJob{t: oldMem, walID: oldWalID, maxSeq: db.seq.Load()})
	oldWal.Close()
	db.cond.Broadcast()
	return nil
}

// flushLoop drains the immutable memtable queue.
func (db *DB) flushLoop() {
	defer db.bgWG.Done()
	for {
		db.mu.Lock()
		for len(db.imm) == 0 && !db.closing {
			db.cond.Wait()
		}
		if len(db.imm) == 0 {
			db.mu.Unlock()
			return
		}
		job := db.imm[0]
		db.imm = db.imm[1:]
		db.mu.Unlock()

		if err := db.flushMemtable(job.t, job.maxSeq); err != nil {
			db.setBgError(err)
		}
		if job.walID != 0 {
			if err := os.Remove(db.logPath(job.walID)); err != nil && !os.IsNotExist(err) {
				db.setBgError(err)
			}
		}
		db.cond.Broadcast() // stalled writers may proceed
		db.wakeCompactor()
	}
}

// flushMemtable writes one memtable out as an L0 table, publishes it
// in the manifest, and drops it from the reader cache bookkeeping.
func (db *DB) flushMemtable(t *mem.Table, through uint64) error {
	if t.Len() == 0 {
		// Nothing to persist; still advance the watermark so waiters
		// see "flushed through".
		db.flushedTo.Store(through)
		return nil
	}
	id := db.allocFileID()
	meta, err := db.writeTable(t.NewIter(), id)
	if err != nil {
		return err
	}
	tm := manifest.TableMeta{
		ID:       id,
		Level:    0,
		Size:     meta.Size,
		Count:    meta.Count,
		Smallest: meta.Smallest,
		Largest:  meta.Largest,
	}
	// NextSeq marks everything assigned so far as flushed; recovery
	// never reassigns these sequences.
	err = db.man.LogAndApply(&manifest.Edit{
		Added:   []manifest.TableMeta{tm},
		NextID:  db.nextFile.Load(),
		NextSeq: db.seq.Load() + 1,
	})
	if err != nil {
		os.Remove(db.sstPath(id))
		return err
	}
	db.flushedTo.Store(through)
	db.stats.flushes.Add(1)
	db.stats.sstWritten.Add(meta.Size)
	return nil
}

// writeTable streams it into sst file id and fsyncs it.
func (db *DB) writeTable(it *mem.Iter, id uint64) (table.Meta, error) {
	f, err := os.OpenFile(db.sstPath(id), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return table.Meta{}, err
	}
	tw := table.NewWriter(f, table.Options{
		BlockSize:   db.opts.BlockSize,
		BitsPerKey:  db.opts.BitsPerKey,
	}) 
	var m table.Meta
	for it.First(); it.Valid(); it.Next() {
		ik, val := it.CurrentRaw()
		tw.Add(ik, val)
	}
	if err := it.Error(); err == nil {
		m, err = tw.Finish()
	}
	if err != nil {
		f.Close()
		tw.Abort()
		os.Remove(db.sstPath(id))
		return table.Meta{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(db.sstPath(id))
		return table.Meta{}, err
	}
	return m, f.Close()
}

// wakeCompactor nudges the background compaction loop.
func (db *DB) wakeCompactor() {
	db.stallMu.Lock()
	db.stallCond.Broadcast()
	db.stallMu.Unlock()
}

// Get returns the newest value for userKey visible at the current
// sequence, or not-found when the newest visible entry is a tombstone.
func (db *DB) Get(userKey []byte) ([]byte, bool, error) {
	return db.GetAt(userKey, db.seq.Load())
}

// GetAt reads at an explicit snapshot sequence.
func (db *DB) GetAt(userKey []byte, snap uint64) ([]byte, bool, error) {
	if err := db.bgFailure(); err != nil {
		return nil, false, err
	}
	v := db.man.Current()
	db.mu.Lock()
	memT := db.mem
	imms := append([]flushJob(nil), db.imm...)
	db.mu.Unlock()

	// Sources in recency order: active memtable, immutable
	// memtables newest first, L0 tables newest first, then the
	// disjoint deeper levels. The first source holding a visible
	// entry for the user key wins: within L0 every entry of a newer
	// file is newer than every entry of an older one, because
	// memtable flushes carry disjoint, increasing sequence ranges.
	if ik, val, ok := memT.Get(userKey, snap); ok {
		return decideGet(ik, val)
	}
	for i := len(imms) - 1; i >= 0; i-- {
		if ik, val, ok := imms[i].t.Get(userKey, snap); ok {
			return decideGet(ik, val)
		}
	}
	if len(v.Levels) > 0 {
		for _, m := range v.Levels[0] {
			r, release, err := db.acquireReader(m.ID)
			if err != nil {
				return nil, false, err
			}
			ik, val, ok, err := r.Get(userKey, snap)
			release()
			if err != nil {
				return nil, false, err
			}
			if ok {
				return decideGet(ik, val)
			}
		}
	}
	for l := 1; l < len(v.Levels); l++ {
		m, ok := v.FindFile(l, userKey)
		if !ok {
			continue
		}
		r, release, err := db.acquireReader(m.ID)
		if err != nil {
			return nil, false, err
		}
		ik, val, ok, err := r.Get(userKey, snap)
		release()
		if err != nil {
			return nil, false, err
		}
		if ok {
			return decideGet(ik, val)
		}
	}
	return nil, false, nil
}

func decideGet(ik key.Internal, val []byte) ([]byte, bool, error) {
	if ik.Type != key.TypeSet {
		return nil, false, nil
	}
	out := make([]byte, len(val))
	copy(out, val)
	return out, true, nil
}

// acquireReader returns a cached table reader and a release func.
// Compaction-retired readers stay open until the last user releases.
func (db *DB) acquireReader(id uint64) (*table.Reader, func(), error) {
	db.readersMu.Lock()
	rr := db.readers[id]
	if rr == nil {
		db.readersMu.Unlock()
		r, err := db.openReader(id)
		if err != nil {
			return nil, nil, err
		}
		db.readersMu.Lock()
		// A racing caller may have opened it first; keep one.
		if rr = db.readers[id]; rr == nil {
			rr = &readerRef{r: r}
			db.readers[id] = rr
		} else {
			r.Close()
		}
	}
	rr.refs++
	db.readersMu.Unlock()
	release := func() {
		db.readersMu.Lock()
		rr.refs--
		if rr.refs == 0 && rr.dead {
			delete(db.readers, id)
			rr.r.Close()
		}
		db.readersMu.Unlock()
	}
	return rr.r, release, nil
}

func (db *DB) openReader(id uint64) (*table.Reader, error) {
	f, err := os.Open(db.sstPath(id))
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	r, err := table.Open(f, uint64(fi.Size()))
	if err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

// retireReaders marks readers dead so their file descriptors close
// once the last in-flight read finishes.
func (db *DB) retireReaders(ids []uint64) {
	db.readersMu.Lock()
	for _, id := range ids {
		if rr := db.readers[id]; rr != nil {
			rr.dead = true
			if rr.refs == 0 {
				delete(db.readers, id)
				rr.r.Close()
			}
		}
	}
	db.readersMu.Unlock()
}

// Close stops background work, waits for pending flushes, and closes
// the log files. The active memtable stays in its WAL; recovery
// replays it.
func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		db.closingFlag.Store(true)
		db.stallCond.Broadcast()
		db.mu.Lock()
		db.closing = true
		db.cond.Broadcast()
		db.mu.Unlock()
		db.bgWG.Wait()
		db.mu.Lock()
		walClose := db.wal.Close()
		db.mu.Unlock()
		manClose := db.man.Close()
		db.readersMu.Lock()
		for id, rr := range db.readers {
			rr.r.Close()
			delete(db.readers, id)
		}
		db.readersMu.Unlock()
		if walClose != nil {
			db.closeErr = walClose
			return
		}
		db.closeErr = manClose
	})
	return db.closeErr
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
