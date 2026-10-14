package manifest

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// Current returns the current version. The returned pointer is
// immutable and safe for concurrent readers.
func (m *Manifest) Current() *Version { return m.cur }

// NextFileID returns the next unused file ID without consuming it.
func (m *Manifest) NextFileID() uint64 { return m.nextID }

// LogAndApply appends the edit to the log, fsyncs, then publishes the
// new version. The edit's Added tables must already be fsynced on
// disk; this call is what makes them live.
func (m *Manifest) LogAndApply(e *Edit) error {
	e = cloneEdit(e)
	// Defensive: file IDs handed out must never be reused even if the
	// caller forgot to carry NextID forward.
	for _, meta := range e.Added {
		if meta.ID >= m.nextID {
			m.nextID = meta.ID + 1
		}
	}
	if e.NextID != 0 && e.NextID > m.nextID {
		m.nextID = e.NextID
	}
	var buf []byte
	for _, meta := range e.Added {
		buf = appendRecord(buf, recAdd, encodeAdd(meta))
	}
	for _, id := range e.Deleted {
		var b []byte
		b = binary.AppendUvarint(b, id)
		buf = appendRecord(buf, recDelete, b)
	}
	var nb []byte
	nb = binary.AppendUvarint(nb, m.nextID)
	buf = appendRecord(buf, recNextID, nb)
	n, err := m.w.Write(buf)
	if err != nil {
		return err
	}
	if err := m.w.Flush(); err != nil {
		return err
	}
	if err := m.f.Sync(); err != nil {
		return err
	}
	m.logBytes += int64(n)
	if err := syncDir(filepath.Dir(m.path)); err != nil {
		return err
	}
	m.cur = apply(m.cur, e)
	if m.logBytes >= m.snapBytes {
		return m.RewriteSnapshot()
	}
	return nil
}

// RewriteSnapshot replaces the log with a single snapshot of the
// current version. File IDs are re-recorded so nextID never regresses
// even when every table that used a high ID has since been dropped.
func (m *Manifest) RewriteSnapshot() error {
	tmp := m.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	var buf []byte
	for _, lv := range m.cur.Levels {
		for _, meta := range lv {
			buf = appendRecord(buf, recAdd, encodeAdd(meta))
		}
	}
	var nb []byte
	nb = binary.AppendUvarint(nb, m.nextID)
	buf = appendRecord(buf, recNextID, nb)
	if _, err := w.Write(buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, m.path); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := syncDir(filepath.Dir(m.path)); err != nil {
		return err
	}
	// The old handle now refers to the replaced inode; reopen for append.
	if err := m.f.Close(); err != nil {
		return err
	}
	f2, err := os.OpenFile(m.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	m.f = f2
	fi, err := f2.Stat()
	if err != nil {
		return err
	}
	m.logBytes = fi.Size()
	m.w.Reset(f2)
	return nil
}

// Close flushes and closes the log.
func (m *Manifest) Close() error {
	if err := m.w.Flush(); err != nil {
		m.f.Close()
		return err
	}
	return m.f.Close()
}

func cloneEdit(e *Edit) *Edit {
	out := &Edit{
		Added:   append([]TableMeta(nil), e.Added...),
		Deleted: append([]uint64(nil), e.Deleted...),
		NextID:  e.NextID,
	}
	for i := range out.Added {
		out.Added[i].Smallest = append([]byte(nil), e.Added[i].Smallest...)
		out.Added[i].Largest = append([]byte(nil), e.Added[i].Largest...)
	}
	return out
}

func encodeAdd(m TableMeta) []byte {
	var b []byte
	b = binary.AppendUvarint(b, m.ID)
	b = binary.AppendUvarint(b, uint64(m.Level))
	b = binary.AppendUvarint(b, m.Size)
	b = binary.AppendUvarint(b, m.Count)
	b = binary.AppendUvarint(b, uint64(len(m.Smallest)))
	b = append(b, m.Smallest...)
	b = binary.AppendUvarint(b, uint64(len(m.Largest)))
	b = append(b, m.Largest...)
	return b
}

// appendRecord frames one payload: u32 LE length, u32 LE crc32c,
// payload.
func appendRecord(dst []byte, tag byte, payload []byte) []byte {
	body := append([]byte{tag}, payload...)
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(body, castagnoli))
	dst = append(dst, hdr[:]...)
	return append(dst, body...)
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

// FileIDs returns every live table ID across all levels, level by
// level. Test and stats helper.
func (v *Version) FileIDs() []uint64 {
	var out []uint64
	for _, lv := range v.Levels {
		for _, m := range lv {
			out = append(out, m.ID)
		}
	}
	return out
}

// String renders a one-line summary of a version.
func (v *Version) String() string {
	total := 0
	live := uint64(0)
	for l, lv := range v.Levels {
		_ = l
		total += len(lv)
		for _, m := range lv {
			live += m.Size
		}
	}
	return fmt.Sprintf("version{tables:%d bytes:%d levels:%d}", total, live, len(v.Levels))
}
