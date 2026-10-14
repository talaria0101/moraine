package db

import (
	"os"

	"github.com/talaria0101/moraine/internal/key"
	"github.com/talaria0101/moraine/internal/manifest"
	"github.com/talaria0101/moraine/internal/table"
)

// compaction describes one merge: inputs[0] from level, inputs[1]
// (optional) from the next level down, output written to target.
type compaction struct {
	level  int
	target int
	inputs [2][]manifest.TableMeta
}

// compactLoop serializes background compactions: pick, execute,
// repeat until no level is over budget. A compaction error is stored
// as the background error and stops the loop; the next foreground
// operation reports it.
func (db *DB) compactLoop() {
	defer db.bgWG.Done()
	for {
		c := db.pickCompaction()
		if c == nil {
			if db.closingFlag.Load() {
				return
			}
			db.stallMu.Lock()
			for db.pickCompaction() == nil && !db.closingFlag.Load() {
				db.stallCond.Wait()
			}
			db.stallMu.Unlock()
			continue
		}
		if db.closingFlag.Load() {
			return
		}
		if err := db.runCompaction(c); err != nil {
			db.setBgError(err)
			return
		}
		db.stats.compactions.Add(1)
	}
}

// pickCompaction chooses the next compaction or nil. L0 fires on file
// count; deeper levels fire when they exceed their size target, one
// file at a time, first key first. This is the default leveled
// trigger; the policy experiments replace it.
func (db *DB) pickCompaction() *compaction {
	v := db.man.Current()
	if len(v.Levels) > 0 && len(v.Levels[0]) >= db.opts.L0CompactionTrigger {
		c := &compaction{level: 0, target: 1}
		c.inputs[0] = append(c.inputs[0], v.Levels[0]...)
		c.inputs[1] = expandOverlaps(v, 1, c.inputs[0])
		return c
	}
	for l := 1; l < db.opts.MaxLevels-1; l++ {
		if l >= len(v.Levels) {
			break
		}
		size := uint64(0)
		for _, m := range v.Levels[l] {
			size += m.Size
		}
		if size <= db.opts.levelTarget(l) || len(v.Levels[l]) == 0 {
			continue
		}
		c := &compaction{level: l, target: l + 1}
		c.inputs[0] = append(c.inputs[0], v.Levels[l][0])
		c.inputs[1] = expandOverlaps(v, l+1, c.inputs[0])
		return c
	}
	return nil
}

// expandOverlaps returns the files of level overlapping the range of
// seeds, expanded to a fixpoint so the output level stays disjoint.
func expandOverlaps(v *manifest.Version, level int, seeds []manifest.TableMeta) []manifest.TableMeta {
	minUser, maxUser := userRange(seeds)
	for {
		got := v.Overlaps(level, minUser, maxUser)
		if len(got) == 0 {
			return nil
		}
		gmin, gmax := userRange(got)
		if key.CompareUser(gmin, minUser) >= 0 && key.CompareUser(gmax, maxUser) <= 0 {
			return got
		}
		minUser, maxUser = gmin, gmax
	}
}

func userRange(ms []manifest.TableMeta) (minUser, maxUser []byte) {
	for i, m := range ms {
		su := userOf(m.Smallest)
		lu := userOf(m.Largest)
		if i == 0 {
			minUser, maxUser = su, lu
			continue
		}
		if key.CompareUser(su, minUser) < 0 {
			minUser = su
		}
		if key.CompareUser(lu, maxUser) > 0 {
			maxUser = lu
		}
	}
	return minUser, maxUser
}

func userOf(ik []byte) []byte {
	if len(ik) < key.TrailerLen {
		return ik
	}
	return ik[:len(ik)-key.TrailerLen]
}

// runCompaction merges the inputs into one or more output files at
// the target level, publishes the manifest edit, then deletes the
// input files. Every version of every user key is preserved, so
// snapshot reads are unaffected; version pruning is the compaction
// policy's business.
func (db *DB) runCompaction(c *compaction) error {
	var srcs []*mergeSrc
	var inputIDs []uint64
	for _, m := range c.inputs[0] {
		if err := appendTable(db, &srcs, &inputIDs, m); err != nil {
			return err
		}
	}
	for _, m := range c.inputs[1] {
		if err := appendTable(db, &srcs, &inputIDs, m); err != nil {
			return err
		}
	}

	var outputs []manifest.TableMeta
	fail := func(err error) error {
		// Drop every output written so far; the manifest was never
		// touched, so the inputs remain authoritative.
		for _, o := range outputs {
			os.Remove(db.sstPath(o.ID))
		}
		return err
	}

	id := db.allocFileID()
	f, w, err := db.openOutput(id)
	if err != nil {
		return err
	}
	outputOpen := false
	var lastUser []byte
	for {
		pick := -1
		for i, s := range srcs {
			if !s.ok {
				continue
			}
			if pick < 0 || key.Compare(s.ik, srcs[pick].ik) < 0 {
				pick = i
			}
		}
		if pick < 0 {
			break
		}
		s := srcs[pick]
		u := userOf(s.ik)
		// Roll the output at file-size boundaries, never inside one
		// user key's versions.
		if outputOpen && w.Size() >= int64(db.opts.TargetFileSize) && key.CompareUser(lastUser, u) != 0 {
			m, ferr := finishTable(db, w, f, c.target, id)
			if ferr != nil {
				return fail(ferr)
			}
			outputs = append(outputs, m)
			outputOpen = false
		}
		if !outputOpen {
			id = db.allocFileID()
			f, w, err = db.openOutput(id)
			if err != nil {
				return fail(err)
			}
			outputOpen = true
		}
		w.Add(s.ik, s.val)
		lastUser = append(lastUser[:0], u...)
		s.prime(false)
		if err := s.it.Error(); err != nil {
			return fail(err)
		}
	}
	if outputOpen {
		m, ferr := finishTable(db, w, f, c.target, id)
		if ferr != nil {
			return fail(ferr)
		}
		outputs = append(outputs, m)
	}

	del := make([]uint64, len(inputIDs))
	copy(del, inputIDs)
	err = db.man.LogAndApply(&manifest.Edit{
		Added:   outputs,
		Deleted: del,
		NextID:  db.nextFile.Load(),
		NextSeq: db.seq.Load() + 1,
	})
	if err != nil {
		return fail(err)
	}
	db.retireReaders(inputIDs)
	for _, id := range inputIDs {
		if err := os.Remove(db.sstPath(id)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func appendTable(db *DB, srcs *[]*mergeSrc, ids *[]uint64, m manifest.TableMeta) error {
	r, release, err := db.acquireReader(m.ID)
	if err != nil {
		return err
	}
	s := &mergeSrc{it: &releaseIter{Iter: r.NewIter(), release: release}}
	s.prime(true)
	*srcs = append(*srcs, s)
	*ids = append(*ids, m.ID)
	return nil
}

// finishTable finishes the writer, fsyncs, closes the file, and
// returns the table metadata for the manifest.
func finishTable(db *DB, w *table.Writer, f *os.File, level int, id uint64) (manifest.TableMeta, error) {
	m, err := w.Finish()
	if err != nil {
		f.Close()
		w.Abort()
		os.Remove(db.sstPath(id))
		return manifest.TableMeta{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(db.sstPath(id))
		return manifest.TableMeta{}, err
	}
	if err := f.Close(); err != nil {
		os.Remove(db.sstPath(id))
		return manifest.TableMeta{}, err
	}
	db.stats.sstWritten.Add(m.Size)
	return manifest.TableMeta{
		ID:       id,
		Level:    level,
		Size:     m.Size,
		Count:    m.Count,
		Smallest: m.Smallest,
		Largest:  m.Largest,
	}, nil
}

// openOutput creates output file id and a writer over it.
func (db *DB) openOutput(id uint64) (*os.File, *table.Writer, error) {
	f, err := os.OpenFile(db.sstPath(id), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, nil, err
	}
	return f, table.NewWriter(f, table.Options{
		BlockSize:  db.opts.BlockSize,
		BitsPerKey: db.opts.BitsPerKey,
	}), nil
}

// releaseIter releases the table reader once the compaction is done
// with the table's iterator.
type releaseIter struct {
	*table.Iter
	release func()
}
