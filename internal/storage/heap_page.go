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
//   slotCount: スロット配列の要素数
//   freeStart: スロット配列の直後の先頭位置
//   freeEnd  : 自由領域の末尾+1（=データは末尾側から詰める）
//   flags   : ページの状態フラグ（将来用）
// 以後に SlotDirectory (各 4B = u16 offset + u16 length)

const (
	hdrSize     = 8      // ヘッダサイズ（バイト）
	slotSize    = 4      // 各スロットエントリのサイズ（バイト）
	flagDeleted = 1 << 0 // 削除フラグ（未使用、将来用）
)

// HeapPage は与えられた 1 ページ分のバイト列に対して
// スロット管理された可変長レコード操作を提供する。
// ページレイアウト:
// [ヘッダ8B][スロット配列][自由領域][データ領域]
type HeapPage struct {
	buf []byte // 長さ == pageSize のページバッファ
}

// NewHeapPage は新しいHeapPageインスタンスを作成する
// バッファが小さすぎる場合はエラーを返す
// 初期化されていないページの場合は自動的に初期化する
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

// Insert は新しいレコードをページに挿入する
// 戻り値: スロットID（成功時）、エラー（失敗時）
// データは末尾側から詰められ、スロットは先頭側に追加される
func (p *HeapPage) Insert(rec []byte) (int, error) {
	need := uint16(len(rec)) + slotSize // レコードサイズ + スロットエントリサイズ
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

// Get は指定されたスロットIDのレコードを取得する
// 戻り値: レコードデータ（存在時）、存在フラグ
// 削除されたスロットの場合は空のスライスとfalseを返す
func (p *HeapPage) Get(slotID int) ([]byte, bool) {
	off, ln, ok := p.slot(slotID)
	if !ok || ln == 0 {
		return nil, false
	}
	return append([]byte(nil), p.buf[off:int(off)+int(ln)]...), true
}

// Delete は指定されたスロットIDのレコードを削除する
// 物理領域はすぐには詰め直さず、スロット長を 0 にする（論理削除）
func (p *HeapPage) Delete(slotID int) error {
	off, ln, ok := p.slot(slotID)
	if !ok || ln == 0 {
		return errors.New("slot not found")
	}
	// 物理領域はすぐには詰め直さず、スロット長を 0 にする（論理削除）
	p.setSlot(slotID, off, 0)
	return nil
}

// Update は指定されたスロットIDのレコードを更新する
// シンプル実装: Delete → 再Insert（スロットIDは変わらない仕様も可能だが、ここでは簡易に）
// ここではスロットIDを維持せず、新規挿入を返す設計にしてもよい。
func (p *HeapPage) Update(slotID int, rec []byte) error {
	if err := p.Delete(slotID); err != nil {
		return err
	}
	_, err := p.Insert(rec)
	return err
}

// freeSpace はページ内の利用可能な自由領域のサイズを返す
// freeStart から freeEnd までの領域サイズを計算
func (p *HeapPage) freeSpace() uint16 {
	fs := int(p.freeEnd()) - int(p.freeStart())
	if fs < 0 {
		return 0
	}
	return uint16(fs)
}

// ---- ヘッダ/スロットアクセス ----

// ヘッダフィールドの読み取りメソッド
func (p *HeapPage) slotCount() uint16 { return binary.LittleEndian.Uint16(p.buf[0:2]) }
func (p *HeapPage) freeStart() uint16 { return binary.LittleEndian.Uint16(p.buf[2:4]) } // 自由領域の先頭位置
func (p *HeapPage) freeEnd() uint16   { return binary.LittleEndian.Uint16(p.buf[4:6]) } // 自由領域の末尾+1
func (p *HeapPage) flags() uint16     { return binary.LittleEndian.Uint16(p.buf[6:8]) } // ページの状態フラグ

// ヘッダフィールドの設定メソッド
func (p *HeapPage) setSlotCount(v uint16) { binary.LittleEndian.PutUint16(p.buf[0:2], v) }
func (p *HeapPage) setFreeStart(v uint16) { binary.LittleEndian.PutUint16(p.buf[2:4], v) }
func (p *HeapPage) setFreeEnd(v uint16)   { binary.LittleEndian.PutUint16(p.buf[4:6], v) }
func (p *HeapPage) setFlags(v uint16)     { binary.LittleEndian.PutUint16(p.buf[6:8], v) }

// slot は指定されたスロットIDのオフセットと長さを取得する
// 戻り値: オフセット、長さ、存在フラグ
func (p *HeapPage) slot(i int) (off uint16, ln uint16, ok bool) {
	if i < 0 || i >= int(p.slotCount()) {
		return 0, 0, false
	}
	base := int(hdrSize) + i*slotSize
	off = binary.LittleEndian.Uint16(p.buf[base : base+2])
	ln = binary.LittleEndian.Uint16(p.buf[base+2 : base+4])
	return off, ln, true
}

// setSlot は指定されたスロットIDのオフセットと長さを設定する
// 無効なスロットインデックスの場合はパニックを発生
func (p *HeapPage) setSlot(i int, off, ln uint16) {
	if i < 0 || i > int(p.slotCount()) {
		panic(fmt.Sprintf("invalid slot index %d", i))
	}
	base := int(hdrSize) + i*slotSize
	binary.LittleEndian.PutUint16(p.buf[base:base+2], off)
	binary.LittleEndian.PutUint16(p.buf[base+2:base+4], ln)
}
