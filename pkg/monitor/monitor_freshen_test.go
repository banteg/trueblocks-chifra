package monitor

import (
	"errors"
	"io"
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
	if err == nil || err.Error() != "download failed" {
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
