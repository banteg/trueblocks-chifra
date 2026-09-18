// Copyright 2021 The TrueBlocks Authors. All rights reserved.
// Use of this source code is governed by a license that can
// be found in the LICENSE file.

package index

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestWriteReaderToPathRejectsConcurrentTruncation(t *testing.T) {
	dir := t.TempDir()
	full := filepath.Join(dir, "000000001-000000002.bin")
	good := bytes.Repeat([]byte("a"), 16)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := writeReaderToPath(full, bytes.NewReader(good), 16, "good"); err != nil {
			t.Errorf("good write: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		_ = writeReaderToPath(full, bytes.NewReader([]byte("bbb")), 16, "short")
	}()
	wg.Wait()

	got, err := os.ReadFile(full)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, good) {
		t.Fatalf("published %q", got)
	}
}

func TestWriteReaderToPathMissingSize(t *testing.T) {
	err := writeReaderToPath(filepath.Join(t.TempDir(), "x.bin"), bytes.NewReader([]byte("abc")), 0, "x")
	if !errors.Is(err, ErrMissingSize) {
		t.Fatalf("err=%v", err)
	}
}
