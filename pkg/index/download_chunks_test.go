// Copyright 2021 The TrueBlocks Authors. All rights reserved.
// Use of this source code is governed by a license that can
// be found in the LICENSE file.

package index

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/types"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/walk"
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
	var shortErr error
	go func() {
		defer wg.Done()
		shortErr = writeReaderToPath(full, bytes.NewReader([]byte("bbb")), 16, "short")
	}()
	wg.Wait()
	if !errors.Is(shortErr, ErrSizeMismatch) {
		t.Fatalf("expected ErrSizeMismatch for short write, got: %v", shortErr)
	}

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

func TestDownloadChunkToDiscRetries(t *testing.T) {
	origDelay := downloadRetryDelay
	downloadRetryDelay = 5 * time.Millisecond
	defer func() { downloadRetryDelay = origDelay }()

	finalBytes := bytes.Repeat([]byte("z"), 64)

	tests := []struct {
		name       string
		firstServe func(w http.ResponseWriter)
	}{
		{
			name: "body smaller than manifest size",
			firstServe: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(finalBytes[:32])
			},
		},
		{
			name: "body interrupted before Content-Length",
			firstServe: func(w http.ResponseWriter) {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", len(finalBytes)))
				w.WriteHeader(http.StatusOK)
				if flusher, ok := w.(http.Flusher); ok {
					_, _ = w.Write(finalBytes[:16])
					flusher.Flush()
				}
				// Close connection ungracefully via panic/hijack or by returning with fewer bytes
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var attempts int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				att := atomic.AddInt32(&attempts, 1)
				if att == 1 {
					tc.firstServe(w)
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(finalBytes)
			}))
			defer server.Close()

			dir := t.TempDir()
			chainDir := filepath.Join(dir, "unchained", "testchain")
			t.Setenv("XDG_CACHE_HOME", dir)

			chunk := types.ChunkRecord{
				Range:     "000000001-000000002",
				IndexSize: int64(len(finalBytes)),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err := downloadChunkToDisc(ctx, cancel, "testchain", walk.Index_Final, chunk, server.URL, "fakehash", 3)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if atomic.LoadInt32(&attempts) < 2 {
				t.Fatalf("expected at least 2 attempts, got %d", attempts)
			}

			destPath := filepath.Join(chainDir, "finalized", chunk.Range+".bin")
			got, err := os.ReadFile(destPath)
			if err != nil {
				t.Fatalf("reading destination: %v", err)
			}
			if !bytes.Equal(got, finalBytes) {
				t.Fatalf("published %q, want %q", got, finalBytes)
			}
		})
	}
}

