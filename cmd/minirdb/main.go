package main

import (
	"fmt"
	"log"
	"os"

	"github.com/minirdb/internal/pager"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: minirdb <dbfile>")
		os.Exit(1)
	}
	dbfile := os.Args[1]

	p, err := pager.Open(dbfile, 4096)
	if err != nil {
		log.Fatalf("Error opening database file: %v", err)
	}
	defer p.Close()

	buf, err := p.ReadPage(0)
	if err != nil {
		log.Fatalf("Error reading page: %v", err)
	}
	copy(buf[:4], []byte{'M', 'R', 'D', 'B'})
	if err := p.WritePage(0, buf); err != nil {
		log.Fatalf("Error writing page: %v", err)
	}
	if err := p.Flush(); err != nil {
		log.Fatalf("Error flushing page: %v", err)
	}

	fmt.Println("OK: wrote magic to page0")
}
