package main

import (
	"fmt"
	"log"
	"os"

	"github.com/k-sml/go-rdbms/internal/pager"
)

func main() {
	// コマンドライン引数の数をチェック（データベースファイル名が必要）
	if len(os.Args) < 2 {
		log.Fatalf("Usage: minirdb <dbfile>")
		os.Exit(1)
	}
	// コマンドライン引数からデータベースファイル名を取得
	dbfile := os.Args[1]

	// ページサイズ4096バイトでデータベースファイルを開く
	p, err := pager.Open(dbfile, 4096)
	if err != nil {
		log.Fatalf("Error opening database file: %v", err)
	}
	// 関数終了時にページャーを確実にクローズ
	defer p.Close()

	// ページ0を読み込み
	buf, err := p.ReadPage(0)
	if err != nil {
		log.Fatalf("Error reading page: %v", err)
	}
	// ページ0の先頭4バイトにマジックナンバー "MRDB" を書き込み
	copy(buf[:4], []byte{'M', 'R', 'D', 'B'})
	// 変更されたページをデータベースファイルに書き込み
	if err := p.WritePage(0, buf); err != nil {
		log.Fatalf("Error writing page: %v", err)
	}
	// メモリ上の変更をディスクにフラッシュ
	if err := p.Flush(); err != nil {
		log.Fatalf("Error flushing page: %v", err)
	}

	// 処理完了のメッセージを出力
	fmt.Println("OK: wrote magic to page0")
}
