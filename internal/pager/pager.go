package pager

import (
	"fmt"
	"io"
	"os"
	"sync"
)

type Pager struct {
	f        *os.File
	pageSize int
	mu       sync.Mutex
}

func Open(path string, pageSize int) (*Pager, error) {
	if pageSize <= 0 || pageSize%512 != 0 {
		return nil, fmt.Errorf("invalid page size: %d", pageSize)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	return &Pager{
		f:        f,
		pageSize: pageSize,
	}, nil
}

func (p *Pager) Close() error {
	return p.f.Close()
}

func (p *Pager) ReadPage(pageID int64) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pageID < 0 {
		return nil, fmt.Errorf("invalid page ID: %d", pageID)
	}

	off := pageID * int64(p.pageSize)
	buf := make([]byte, p.pageSize)

	st, err := p.f.Stat()
	if err != nil {
		return nil, err
	}
	if off >= st.Size() {
		if err := p.ensureSize(off + int64(p.pageSize)); err != nil {
			return nil, err
		}
		return buf, nil
	}

	if _, err := p.f.ReadAt(buf, off); err != nil && err != io.EOF {
		return nil, err
	}

	return buf, nil
}

func (p *Pager) WritePage(pageID int64, buf []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(buf) != p.pageSize {
		return fmt.Errorf("invalid page size: %d", len(buf))
	}

	off := pageID * int64(p.pageSize)

	if err := p.ensureSize(off + int64(p.pageSize)); err != nil {
		return err
	}

	if _, err := p.f.WriteAt(buf, off); err != nil {
		return err
	}

	return nil

}

func (p *Pager) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.f.Sync()
}

func (p *Pager) PageSize() int { return p.pageSize }

func (p *Pager) ensureSize(n int64) error {
	st, err := p.f.Stat()
	if err != nil {
		return err
	}
	if st.Size() >= n {
		return nil
	}
	if err := p.f.Truncate(n); err != nil {
		return err
	}
	return nil
}
