package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestSplitFile(t *testing.T) {
	testSplitFileFunc(t, "testdata/input.txt", func(outdir string) error {
		return splitFile(4, 1, "testdata/input.txt", filepath.Join(outdir, "input"))
	})
}

func TestSplitFileParallel(t *testing.T) {
	testSplitFileFunc(t, "testdata/input.txt", func(outdir string) error {
		return splitFileParallel(context.Background(), 4, 6, 1, "testdata/input.txt", filepath.Join(outdir, "input"))
	})
}

// testSplitFileFunc is used to test both splitFile and splitFileParallel
func testSplitFileFunc(t testing.TB, filename string, split func(string) error) {
	outdir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("creating temp dir: %v", err)
	}
	//t.Logf("Using temp folder %q", outdir)
	defer os.RemoveAll(outdir)

	// Generate test output files
	err = split(outdir)
	if err != nil {
		t.Fatal(err)
	}

	// Check output files
	var out []byte
	entries, err := os.ReadDir(outdir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		// t.Log(entry.Name())
		chunk, err := os.ReadFile(filepath.Join(outdir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, chunk...)
	}

	// Compare with original input
	in, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(in, out) {
		t.Errorf("Output (%d bytes) differs from input (%d bytes)", len(in), len(out))
	}

	// TODO if input and output differ only from newlines, consider
	// ignoring trailing newlines, or normalizing CRLF newlines.
}
