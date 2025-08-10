package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ページサイズは Pager 側の値と一致させる想定。ここでは 4096 をデフォルトに。
const DefaultPageSize = 4096

// ヘッダレイアウト（先頭から固定長）
// [u16:slotCount][u16:freeStart][u16:freeEnd][u16:flags]
//   freeStart: スロット配列の直後の先頭位置
//   freeEnd  : 自由領域の末尾+1（=データは末尾側から詰める）
// 以後に SlotDirectory (各 4B = u16 offset + u16 length)

const (
	hdrSize     = 8
	slotSize    = 4
	flagDeleted = 1 << 0 // 未使用（将来用）
)

// HeapPage は与えられた 1 ページ分のバイト列に対して
// スロット管理された可変長レコード操作を提供する。
type HeapPage struct {
	buf []byte // 長さ == pageSize
}

func NewHeapPage(buf []byte) (*HeapPage, error) {
	if len(buf) < hdrSize {
		return nil, errors.New("page buffer too small")
	}
	hp := &HeapPage{buf: buf}
	if hp.slotCount() == 0 && hp.freeStart() == 0 && hp.freeEnd() == 0 {
		// 初期化されていないページとみなす → 初期化
		hp.setSlotCount(0)
		hp.setFreeStart(hdrSize)
		hp.setFreeEnd(uint16(len(buf)))
		hp.setFlags(0)
	}
	return hp, nil
}

// Public API
func (p *HeapPage) Insert(rec []byte) (int, error) {
	need := uint16(len(rec)) + slotSize
	if p.freeSpace() < need {
		return -1, errors.New("page is full")
	}
	// データは末尾側から詰める
	newEnd := p.freeEnd() - uint16(len(rec))
	copy(p.buf[newEnd:p.freeEnd()], rec)

	// スロットを末尾に追加（スロット配列は先頭側へ伸長）
	slotID := int(p.slotCount())
	p.setSlotCount(p.slotCount() + 1)
	p.setFreeStart(p.freeStart() + slotSize)
	p.setSlot(slotID, newEnd, uint16(len(rec)))

	p.setFreeEnd(newEnd)
	return slotID, nil
}

func (p *HeapPage) Get(slotID int) ([]byte, bool) {
	off, ln, ok := p.slot(slotID)
	if !ok || ln == 0 {
		return nil, false
	}
	return append([]byte(nil), p.buf[off:int(off)+int(ln)]...), true
}

func (p *HeapPage) Delete(slotID int) error {
	off, ln, ok := p.slot(slotID)
	if !ok || ln == 0 {
		return errors.New("slot not found")
	}
	// 物理領域はすぐには詰め直さず、スロット長を 0 にする（論理削除）
	p.setSlot(slotID, off, 0)
	return nil
}

func (p *HeapPage) Update(slotID int, rec []byte) error {
	// シンプル実装: Delete → 再Insert（スロットIDは変わらない仕様も可能だが、ここでは簡易に）
	// ここではスロットIDを維持せず、新規挿入を返す設計にしてもよい。
	if err := p.Delete(slotID); err != nil {
		return err
	}
	_, err := p.Insert(rec)
	return err
}

func (p *HeapPage) freeSpace() uint16 {
	fs := int(p.freeEnd()) - int(p.freeStart())
	if fs < 0 {
		return 0
	}
	return uint16(fs)
}

// ---- ヘッダ/スロットアクセス ----
func (p *HeapPage) slotCount() uint16     { return binary.LittleEndian.Uint16(p.buf[0:2]) }
func (p *HeapPage) freeStart() uint16     { return binary.LittleEndian.Uint16(p.buf[2:4]) }
func (p *HeapPage) freeEnd() uint16       { return binary.LittleEndian.Uint16(p.buf[4:6]) }
func (p *HeapPage) flags() uint16         { return binary.LittleEndian.Uint16(p.buf[6:8]) }
func (p *HeapPage) setSlotCount(v uint16) { binary.LittleEndian.PutUint16(p.buf[0:2], v) }
func (p *HeapPage) setFreeStart(v uint16) { binary.LittleEndian.PutUint16(p.buf[2:4], v) }
func (p *HeapPage) setFreeEnd(v uint16)   { binary.LittleEndian.PutUint16(p.buf[4:6], v) }
func (p *HeapPage) setFlags(v uint16)     { binary.LittleEndian.PutUint16(p.buf[6:8], v) }

func (p *HeapPage) slot(i int) (off uint16, ln uint16, ok bool) {
	if i < 0 || i >= int(p.slotCount()) {
		return 0, 0, false
	}
	base := int(hdrSize) + i*slotSize
	off = binary.LittleEndian.Uint16(p.buf[base : base+2])
	ln = binary.LittleEndian.Uint16(p.buf[base+2 : base+4])
	return off, ln, true
}

func (p *HeapPage) setSlot(i int, off, ln uint16) {
	if i < 0 || i > int(p.slotCount()) {
		panic(fmt.Sprintf("invalid slot index %d", i))
	}
	base := int(hdrSize) + i*slotSize
	binary.LittleEndian.PutUint16(p.buf[base:base+2], off)
	binary.LittleEndian.PutUint16(p.buf[base+2:base+4], ln)
}
