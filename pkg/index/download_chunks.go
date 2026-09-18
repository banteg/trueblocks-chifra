// Copyright 2021 The TrueBlocks Authors. All rights reserved.
// Use of this source code is governed by a license that can
// be found in the LICENSE file.

package index

// Fetching, unzipping, validating and saving both index and bloom chunks

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/colors"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/config"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/debug"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/file"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/logger"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/progress"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/sigintTrap"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/types"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/utils"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/walk"
	ants "github.com/panjf2000/ants/v2"
)

// jobResult type is used to carry both downloaded data and some
// metadata to decompressing/file writing function through a channel
type jobResult struct {
	rng      string
	fileSize int64
	contents io.Reader
	theChunk *types.ChunkRecord
}

type progressChan chan<- *progress.ProgressMsg

// Types of errors put into the progressChannel

var ErrFailedLocalFileRemoval = errors.New("failed to remove local file")
var ErrUserHitControlC = errors.New("user hit control + c")
var ErrDownloadError = errors.New("download error")
var ErrWriteToDiscError = errors.New("write to disc error")
var ErrSizeMismatch = errors.New("downloaded chunk size mismatch")
var ErrMissingSize = errors.New("missing expected chunk size")

// WorkerArguments are types meant to hold worker function arguments. We cannot
// pass the arguments directly, because a worker function is expected to take one
// parameter of type interface{}.
type downloadWorkerArguments struct {
	ctx             context.Context
	cancel          context.CancelFunc
	progressChannel progressChan
	gatewayUrl      string
	downloadWg      *sync.WaitGroup
	nRetries        int
}

// worker function type as accepted by Ants
type workerFunction func(interface{})

// getDownloadWorker returns a worker function that downloads a chunk and writes it to disc
func getDownloadWorker(chain string, workerArgs downloadWorkerArguments, chunkType walk.CacheType) workerFunction {
	progressChannel := workerArgs.progressChannel

	return func(param interface{}) {
		chunk := param.(types.ChunkRecord)

		defer workerArgs.downloadWg.Done()

		select {
		case <-workerArgs.ctx.Done():
			return

		default:
			hash := chunk.BloomHash
			if chunkType == walk.Index_Final {
				hash = chunk.IndexHash
			}
			if hash == "" {
				return
			}

			// TODO: Do we really need the colored display?
			bHash := utils.FormattedHash(false, chunk.BloomHash.String())
			iHash := utils.FormattedHash(false, chunk.IndexHash.String())
			tHash := utils.FormattedHash(false, hash.String())
			msg := fmt.Sprintf("%s %s %s", chunk.Range, bHash, iHash)
			msg = strings.ReplaceAll(msg, tHash, colors.BrightCyan+tHash+colors.Off)
			progressChannel <- &progress.ProgressMsg{
				Payload: &chunk,
				Event:   progress.Start,
				Message: msg,
			}

			err := downloadChunkToDisc(workerArgs.ctx, workerArgs.cancel, chain, chunkType, chunk, workerArgs.gatewayUrl, hash.String(), workerArgs.nRetries)
			if errors.Is(workerArgs.ctx.Err(), context.Canceled) {
				return
			}
			if workerArgs.ctx.Err() != nil {
				chunkPath := filepath.Join(config.PathToIndex(chain), "finalized", chunk.Range+".bin")
				removeLocalFile(ToIndexPath(chunkPath), "user canceled", progressChannel)
				removeLocalFile(ToBloomPath(chunkPath), "user canceled", progressChannel)
				progressChannel <- &progress.ProgressMsg{
					Payload: &chunk,
					Event:   progress.Error,
					Error:   fmt.Errorf("%w [%s]", ErrUserHitControlC, workerArgs.ctx.Err().Error()),
				}
				return
			}
			if err != nil {
				progressChannel <- &progress.ProgressMsg{
					Payload: &chunk,
					Event:   progress.Error,
					Error:   fmt.Errorf("%w [%s]", ErrDownloadError, err.Error()),
				}
				return
			}
			progressChannel <- &progress.ProgressMsg{
				Payload: &chunk,
				Event:   progress.Finished,
				Message: chunkType.String(),
			}
		}
	}
}

// fetchResult type make it easier to return both download content and
// download size information (for validation purposes)
type fetchResult struct {
	Body       io.ReadCloser
	ContentLen int64 // download size in bytes
}

func downloadChunkToDisc(ctx context.Context, cancel context.CancelFunc, chain string, chunkType walk.CacheType, chunk types.ChunkRecord, gateway, hash string, nRetries int) error {
	if nRetries < 1 {
		nRetries = 1
	}
	delay := downloadRetryDelay
	var lastErr error
	for attempt := 1; attempt <= nRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := func() error {
			download, err := fetchFromIpfsGateway(ctx, gateway, hash)
			if err != nil {
				return err
			}
			defer download.Body.Close()
			res := &jobResult{
				rng:      chunk.Range,
				fileSize: download.ContentLen,
				contents: download.Body,
				theChunk: &chunk,
			}
			cleanOnQuit := func() {
				logger.Warn(sigintTrap.TrapMessage)
			}
			trapChannel := sigintTrap.Enable(ctx, cancel, cleanOnQuit)
			defer sigintTrap.Disable(trapChannel)
			return writeBytesToDisc(chain, chunkType, res)
		}()
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryableDownloadErr(lastErr) || attempt == nRetries {
			break
		}
		logger.Warn("Failed download", chunk.Range, "(will retry)", attempt, "of", nRetries)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay > 0 && delay < 8*time.Second {
			delay *= 2
		}
	}
	return lastErr
}

func retryableDownloadErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, ErrUserHitControlC) || errors.Is(err, ErrMissingSize) {
		return false
	}
	if os.IsPermission(err) {
		return false
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		if pathErr.Err == syscall.ENOSPC || pathErr.Err == syscall.EACCES || pathErr.Err == syscall.EPERM {
			return false
		}
	}
	return true
}

var downloadRetryDelay = time.Second

// fetchFromIpfsGateway downloads a chunk from an IPFS gateway using HTTP
func fetchFromIpfsGateway(ctx context.Context, gateway, hash string) (*fetchResult, error) {
	url, _ := url.Parse(gateway)
	url.Path = path.Join(url.Path, hash)

	debug.DebugCurlStr(url.String())
	request, err := http.NewRequestWithContext(ctx, "GET", url.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("NewRequestWithContext %s returned error: %w", url, err)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("DefaultClient.Do %s returned error: %w", url, err)
	}

	if response.StatusCode != 200 {
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		return nil, fmt.Errorf("fetchFromIpfsGateway %s returned status code: %d", url, response.StatusCode)
	}

	contentLen := int64(0)
	if len(response.Header.Get("Content-Length")) != 0 {
		contentLen, err = strconv.ParseInt(response.Header.Get("Content-Length"), 10, 64)
		if err != nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			return nil, fmt.Errorf("response.Header.Get %s returned error: %w", url, err)
		}
	}

	return &fetchResult{
		Body:       response.Body,
		ContentLen: contentLen,
	}, nil
}

// DownloadChunks downloads, unzips and saves the chunk of type indicated by chunkType
// for each chunk in chunks. ProgressMsg is reported to progressChannel.
func DownloadChunks(chain string, chunksToDownload []types.ChunkRecord, chunkType walk.CacheType, poolSize int, progressChannel progressChan) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var downloadWg sync.WaitGroup
	downloadWorkerArgs := downloadWorkerArguments{
		ctx:             ctx,
		cancel:          cancel,
		progressChannel: progressChannel,
		downloadWg:      &downloadWg,
		gatewayUrl:      config.GetChain(chain).IpfsGateway,
		nRetries:        8,
	}
	downloadPool, err := ants.NewPoolWithFunc(poolSize, getDownloadWorker(chain, downloadWorkerArgs, chunkType))
	defer downloadPool.Release()
	if err != nil {
		logger.Panic(err)
	}

	for _, chunk := range chunksToDownload {
		downloadWg.Add(1)
		_ = downloadPool.Invoke(chunk)
	}
	downloadWg.Wait()

	if errors.Is(ctx.Err(), context.Canceled) {
		progressChannel <- &progress.ProgressMsg{
			Event: progress.Cancelled,
		}
		return
	}

	progressChannel <- &progress.ProgressMsg{
		Event: progress.AllDone,
	}
}

// writeBytesToDisc save the downloaded bytes to disc
func writeBytesToDisc(chain string, chunkType walk.CacheType, res *jobResult) error {
	fullPath := filepath.Join(config.PathToIndex(chain), "finalized", res.rng+".bin")
	if chunkType == walk.Index_Bloom {
		fullPath = ToBloomPath(fullPath)
	}
	return writeReaderToPath(fullPath, res.contents, expectedChunkSize(chunkType, res), res.rng)
}

func writeReaderToPath(fullPath string, contents io.Reader, expected int64, rng string) error {
	if expected <= 0 {
		return fmt.Errorf("%w for %s", ErrMissingSize, rng)
	}
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}

	outputFile, err := os.CreateTemp(filepath.Dir(fullPath), filepath.Base(fullPath)+".download-*.tmp")
	if err != nil {
		return fmt.Errorf("error creating download temp file for %s in writeBytesToDisc: [%w]", rng, err)
	}
	tmpPath := outputFile.Name()
	success := false
	defer func() {
		if !success {
			_ = os.Remove(tmpPath)
		}
	}()
	_ = outputFile.Chmod(0666)

	written, err := io.Copy(outputFile, contents)
	closeErr := outputFile.Close()
	if err != nil {
		col := colors.Magenta
		if fullPath == ToIndexPath(fullPath) {
			col = colors.Yellow
		}
		logger.Warn("Failed download", col, rng, colors.Off, strings.Repeat(" ", 30))
		// Information about this error
		// https://community.k6.io/t/warn-0040-request-failed-error-stream-error-stream-id-3-internal-error/777/2
		return fmt.Errorf("error copying %s file in writeBytesToDisc: [%s]", rng, err)
	}
	if closeErr != nil {
		return fmt.Errorf("error closing %s file in writeBytesToDisc: [%s]", rng, closeErr)
	}
	if written != expected {
		return fmt.Errorf("%w for %s: wrote %d, expected %d", ErrSizeMismatch, rng, written, expected)
	}
	if err := os.Rename(tmpPath, fullPath); err != nil {
		return fmt.Errorf("error renaming %s file in writeBytesToDisc: [%s]", rng, err)
	}
	success = true
	return nil
}

func expectedChunkSize(chunkType walk.CacheType, res *jobResult) int64 {
	if res == nil {
		return 0
	}
	if res.theChunk != nil {
		if chunkType == walk.Index_Bloom && res.theChunk.BloomSize > 0 {
			return res.theChunk.BloomSize
		}
		if chunkType != walk.Index_Bloom && res.theChunk.IndexSize > 0 {
			return res.theChunk.IndexSize
		}
	}
	return res.fileSize
}

func removeLocalFile(fullPath, reason string, progressChannel progressChan) bool {
	if file.FileExists(fullPath) {
		err := os.Remove(fullPath)
		if err != nil {
			progressChannel <- &progress.ProgressMsg{
				Event: progress.Error,
				Error: fmt.Errorf("%w %s [%s]", ErrFailedLocalFileRemoval, fullPath, err.Error()),
			}
		} else {
			progressChannel <- &progress.ProgressMsg{
				Event:   progress.Update,
				Message: fmt.Sprintf("File %s removed [%s]", fullPath, reason),
			}
		}
	}
	return file.FileExists(fullPath)
}
