package index

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/base"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/file"
)

func writeTestIndex(t *testing.T, path string, nAddr, nApp uint32, extra []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	hdr := indexHeader{Magic: file.MagicNumber, AddressCount: nAddr, AppearanceCount: nApp}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(extra); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenIndexRejectsTruncatedTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000000001-000000002.bin")
	writeTestIndex(t, path, 1, 1, nil)
	_, err := OpenIndex(path, false)
	if !errors.Is(err, ErrCorruptIndex) {
		t.Fatalf("err=%v", err)
	}
}

func TestSearchForAddressRecordIOError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000000001-000000002.bin")
	tables := make([]byte, AddrRecordWidth)
	writeTestIndex(t, path, 1, 0, tables)
	idx, err := OpenIndex(path, false)
	if err != nil {
		t.Fatal(err)
	}
	idx.File.Close()
	_, err = idx.searchForAddressRecord(base.Address{})
	if err == nil {
		t.Fatal("expected seek/read error")
	}
}
