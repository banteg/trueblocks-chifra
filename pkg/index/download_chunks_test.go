// Copyright 2021 The TrueBlocks Authors. All rights reserved.
// Use of this source code is governed by a license that can
// be found in the LICENSE file.

package index

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/types"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/walk"
)

func TestExpectedChunkSize(t *testing.T) {
	chunk := &types.ChunkRecord{IndexSize: 100, BloomSize: 40}
	res := &jobResult{fileSize: 7, theChunk: chunk}
	if got := expectedChunkSize(walk.Index_Final, res); got != 100 {
		t.Fatalf("index size: got %d", got)
	}
	if got := expectedChunkSize(walk.Index_Bloom, res); got != 40 {
		t.Fatalf("bloom size: got %d", got)
	}

	fallback := &jobResult{fileSize: 7, theChunk: &types.ChunkRecord{}}
	if got := expectedChunkSize(walk.Index_Final, fallback); got != 7 {
		t.Fatalf("content-length fallback: got %d", got)
	}
	unknown := &jobResult{theChunk: &types.ChunkRecord{}}
	if got := expectedChunkSize(walk.Index_Final, unknown); got != 0 {
		t.Fatalf("unknown size should fail closed: got %d", got)
	}
}

func TestFetchWithRetries(t *testing.T) {
	old := downloadRetryDelay
	downloadRetryDelay = 0
	defer func() { downloadRetryDelay = old }()

	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if n < 3 {
			http.Error(w, "nope", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	res, err := fetchWithRetries(context.Background(), srv.URL, "cid", 8)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if string(body) != "ok" {
		t.Fatalf("body=%q", body)
	}
	if n != 3 {
		t.Fatalf("attempts=%d", n)
	}
}

func TestFetchWithRetriesHonorsLimit(t *testing.T) {
	old := downloadRetryDelay
	downloadRetryDelay = 0
	defer func() { downloadRetryDelay = old }()

	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		http.Error(w, "nope", http.StatusBadGateway)
	}))
	defer srv.Close()

	_, err := fetchWithRetries(context.Background(), srv.URL, "cid", 2)
	if err == nil {
		t.Fatal("expected error")
	}
	if n != 2 {
		t.Fatalf("attempts=%d", n)
	}
}

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

func TestRetryableDownloadErr(t *testing.T) {
	if retryableDownloadErr(ErrMissingSize) {
		t.Fatal("missing size is permanent")
	}
	if retryableDownloadErr(ErrUserHitControlC) {
		t.Fatal("cancel is permanent")
	}
	if !retryableDownloadErr(ErrSizeMismatch) {
		t.Fatal("short body should retry")
	}
	if !retryableDownloadErr(errors.New("gateway 502")) {
		t.Fatal("remote errors should retry")
	}
}
