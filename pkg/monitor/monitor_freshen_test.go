package monitor

import (
	"encoding/binary"
	"errors"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/base"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/config"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/file"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/index"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/logger"
	"github.com/TrueBlocks/trueblocks-chifra/v6/pkg/ranges"
)

func TestPartitionFreshenResultsSkipsPastHole(t *testing.T) {
	w := logger.GetLoggerWriter()
	defer logger.SetLoggerWriter(w)
	logger.SetLoggerWriter(io.Discard)

	results := []index.AppearanceResult{
		{Range: ranges.FileRange{First: 200, Last: 299}},
		{Range: ranges.FileRange{First: 100, Last: 199}, Err: errors.New("download failed")},
		{Range: ranges.FileRange{First: 0, Last: 99}},
	}
	keep, err := partitionFreshenResults(results)
	if err == nil || err.Error() != "000000100-000000199: download failed" {
		t.Fatalf("err=%v", err)
	}
	if len(keep) != 1 || keep[0].Range.First != 0 || keep[0].Range.Last != 99 {
		t.Fatalf("keep=%v", keep)
	}
}

func TestPartitionFreshenResultsNoErrorKeepsAll(t *testing.T) {
	results := []index.AppearanceResult{
		{Range: ranges.FileRange{First: 200, Last: 299}},
		{Range: ranges.FileRange{First: 0, Last: 99}},
	}
	keep, err := partitionFreshenResults(results)
	if err != nil {
		t.Fatal(err)
	}
	if len(keep) != 2 {
		t.Fatalf("keep=%v", keep)
	}
	if keep[0].Range.First != 0 || keep[1].Range.First != 200 {
		t.Fatalf("order=%v", keep)
	}
}

func TestPartitionFreshenResultsDropsWholeFailedRange(t *testing.T) {
	w := logger.GetLoggerWriter()
	defer logger.SetLoggerWriter(w)
	logger.SetLoggerWriter(io.Discard)

	results := []index.AppearanceResult{
		{Range: ranges.FileRange{First: 100, Last: 199}},
		{Range: ranges.FileRange{First: 100, Last: 199}, Err: errors.New("eof")},
		{Range: ranges.FileRange{First: 0, Last: 99}},
	}
	keep, err := partitionFreshenResults(results)
	if err == nil || err.Error() != "000000100-000000199: eof" {
		t.Fatalf("err=%v", err)
	}
	if len(keep) != 1 || keep[0].Range.First != 0 {
		t.Fatalf("keep=%v", keep)
	}
}

func TestFreshenMonitorsStopsAtCorruptChunkAndResumes(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	const chain = "testchain"
	address := base.HexToAddress("0x1234567890123456789012345678901234567890")
	root := config.PathToIndex(chain)
	hash := config.HeaderHash(config.ExpectedVersion())
	writeChunk := func(first uint32) string {
		t.Helper()
		rng := ranges.FileRange{First: base.Blknum(first), Last: base.Blknum(first + 99)}
		// A bloom with every bit set guarantees that the index is consulted.
		bloom := make([]byte, 42+index.BLOOM_WIDTH_IN_BYTES)
		binary.LittleEndian.PutUint16(bloom, file.SmallMagicNumber)
		copy(bloom[2:34], hash)
		binary.LittleEndian.PutUint32(bloom[34:], 1)
		for i := 42; i < len(bloom); i++ {
			bloom[i] = 0xff
		}
		path := filepath.Join(root, "finalized", rng.String()+".bin")
		if err := os.WriteFile(index.ToBloomPath(path), bloom, 0600); err != nil {
			t.Fatal(err)
		}
		data := make([]byte, index.HeaderWidth+index.AddrRecordWidth+index.AppRecordWidth)
		binary.LittleEndian.PutUint32(data, file.MagicNumber)
		copy(data[4:36], hash)
		binary.LittleEndian.PutUint32(data[36:], 1)
		binary.LittleEndian.PutUint32(data[40:], 1)
		copy(data[44:64], address.Bytes())
		binary.LittleEndian.PutUint32(data[68:], 1)
		binary.LittleEndian.PutUint32(data[72:], first+1)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	writeChunk(0)
	broken := writeChunk(100)
	writeChunk(200)
	if err := os.Truncate(broken, index.HeaderWidth); err != nil {
		t.Fatal(err)
	}
	freshen := func() error {
		updater := NewUpdater(chain, true, false, []string{address.Hex()})
		updater.MaxTasks = 3
		monitors := make([]Monitor, 0, 1)
		_, err := updater.FreshenMonitors(&monitors)
		return err
	}
	checkMonitor := func(last uint32, count int64) {
		t.Helper()
		mon, err := NewMonitor(chain, address, false)
		if err != nil {
			t.Fatal(err)
		}
		defer mon.Close()
		if err := mon.ReadMonitorHeader(); err != nil {
			t.Fatal(err)
		}
		if mon.LastScanned != last || mon.Count() != count {
			t.Fatalf("lastScanned=%d count=%d, want %d %d", mon.LastScanned, mon.Count(), last, count)
		}
	}
	if err := freshen(); !errors.Is(err, index.ErrCorruptIndex) {
		t.Fatalf("expected corruption error, got %v", err)
	}
	checkMonitor(100, 1)
	if _, err := os.Stat(broken + ".corrupt"); err != nil {
		t.Fatalf("missing quarantined chunk: %v", err)
	}
	writeChunk(100)
	if err := freshen(); err != nil {
		t.Fatal(err)
	}
	checkMonitor(300, 3)
}
