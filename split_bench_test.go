package main

import (
	"context"
	"path/filepath"
	"testing"
)

func BenchmarkSplitFile(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testSplitFileFunc(b, "testdata/input_2MB.txt", func(outdir string) error {
			return splitFile(4, 1, "testdata/input_2MB.txt", filepath.Join(outdir, "input_2M"))
		})
	}
}

func BenchmarkSplitFileParallel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testSplitFileFunc(b, "testdata/input_2MB.txt", func(outdir string) error {
			return splitFileParallel(context.Background(), 4, 6, 1, "testdata/input_2MB.txt", filepath.Join(outdir, "input_2M"))
		})
	}
}
