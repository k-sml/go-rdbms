// Package pager はデータベース管理のためのページベースのファイルI/O操作を提供します。
// 固定サイズのページをディスクファイルから読み書きし、
// スレッドセーフな操作を処理します。
package pager

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// Pager はページベースのファイルI/O操作を管理します。
// 固定サイズのページに分割されたファイルへのスレッドセーフなアクセスを提供します。
type Pager struct {
	f        *os.File   // 基となるファイルハンドル
	pageSize int        // 各ページのサイズ（バイト）
	mu       sync.Mutex // スレッドセーフ操作のためのミューテックス
}

// Open は指定されたファイルパスの新しいPagerインスタンスを作成します。
// pageSizeは正の値で、512バイトの倍数である必要があります。
// ファイルが開けない場合やpageSizeが無効な場合はエラーを返します。
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

// Close は基となるファイルを閉じてリソースを解放します。
func (p *Pager) Close() error {
	return p.f.Close()
}

// ReadPage は指定されたpageIDのページをディスクから読み込みます。
// ページが存在しない場合、新しい空のページを作成します。
// ページデータをバイトスライスとして返すか、操作が失敗した場合はエラーを返します。
func (p *Pager) ReadPage(pageID int64) ([]byte, error) {
	// ミューテックスを取得、ロックされている間は他のスレッドがこのメソッドを呼び出せないようにする
	p.mu.Lock()
	defer p.mu.Unlock()

	if pageID < 0 {
		return nil, fmt.Errorf("invalid page ID: %d", pageID)
	}

	off := pageID * int64(p.pageSize) // オフセットは何文字目から読むか
	buf := make([]byte, p.pageSize)   // ページサイズ分のバイトスライスを作成、このバッファにファイルから読み込んだデータを格納する

	st, err := p.f.Stat() // ファイルサイズの確認
	if err != nil {
		return nil, err
	}
	// 書き込みの際も最初にDBの様子を知るためにReadPageを呼び出す、その場合、これに引っかかることがある
	if off >= st.Size() { // ファイルサイズよりオフセットが大きい場合、ファイルサイズを拡張する
		if err := p.ensureSize(off + int64(p.pageSize)); err != nil {
			return nil, err
		}
		return buf, nil
	}

	if _, err := p.f.ReadAt(buf, off); err != nil && err != io.EOF { // ファイルからバッファに読み込み、EOFでない場合はエラーを返す
		return nil, err
	}

	return buf, nil
}

// WritePage は指定されたpageIDのページをディスクに書き込みます。
// バッファサイズはページサイズと正確に一致する必要があります。
// 書き込み操作が失敗した場合はエラーを返します。
func (p *Pager) WritePage(pageID int64, buf []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(buf) != p.pageSize { // バッファサイズはページサイズと正確に一致する必要がある
		return fmt.Errorf("invalid page size: %d", len(buf))
	}

	off := pageID * int64(p.pageSize) // 何文字目から書き込むか

	if err := p.ensureSize(off + int64(p.pageSize)); err != nil {
		return err
	}

	if _, err := p.f.WriteAt(buf, off); err != nil {
		return err
	}

	return nil

}

// Flush は保留中のすべての書き込みがディスクに書き込まれることを保証します。
// 重要な操作の前にデータの永続性を確保するのに役立ちます。
func (p *Pager) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.f.Sync()
}

// PageSize は各ページのサイズをバイトで返します。
func (p *Pager) PageSize() int { return p.pageSize }

// ensureSize はファイルが少なくともnバイトの長さであることを保証します。
// ファイルが短い場合、ゼロで拡張します。
// これはReadPageとWritePageで使用される内部ヘルパーメソッドです。
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
