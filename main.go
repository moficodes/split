package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	filename   string
	count      int
	goroutine  int
	version    bool
	ver        string
	buffer     int
	parallel   bool
	linelength int
	VERSION    string = "v0.0.0"
)

func init() {
	flag.StringVar(&filename, "filename", "input.txt", "file name to split")
	flag.IntVar(&count, "count", 0, "split the file in these many files")
	flag.BoolVar(&version, "version", false, "show version")
	flag.IntVar(&buffer, "buffer", 1, "buffer size in MB")
	flag.IntVar(&linelength, "linelength", 17, "length of each line (length of each number + 1 for newline)")
	flag.BoolVar(&parallel, "parallel", false, "split the file in parallel (default false)")
	flag.IntVar(&goroutine, "goroutine", runtime.GOMAXPROCS(-1), "number of concurrent workers")
}

func copyChunk(in io.Reader, out io.ReaderFrom, n int64) (int64, error) {
	// ReaderFrom is a Writer that has the "Read from..." capability
	part := io.LimitReader(in, n)
	return out.ReadFrom(part)
}

func splitFile(count, buffer int, filename, filenamePrefix string) error {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()
	return split(count, buffer, f, fileSize, filenamePrefix)
}

func split(count, bufferMB int, f io.ReadSeeker, fileSize int64, filenamePrefix string) error {
	// each line is 17 bytes
	// so we can calculate the number of lines per chunk
	linesPerChunk := int((fileSize / int64(linelength)) / int64(count))
	// each chunk is 17 bytes per line (16 digits + 1 newline)
	// we need to do it this way to avoid any rounding errors
	// for example if we have say 100 lines. that is 1700 bytes
	// if we want to split it into 3 files. then each file should have 566 bytes
	// 566 bytes does not divide in 17 bytes per line.
	// instead if we calculate lines per chunk it comes to be 33
	// then reach chunk size is 660 bytes exactly
	chunkSize := linesPerChunk * linelength

	for i := 0; i < count; i++ {
		_, err := f.Seek(int64(chunkSize*i), io.SeekStart)
		if err != nil {
			return err
		}
		file, err := os.OpenFile(fmt.Sprintf("%s_%04d.txt", filenamePrefix, i+1), os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		// TODO use bufio to take "bufferMB" into account

		if i == count-1 {
			// Write everything we have left
			// The last file may be larger than the previous chunks!
			_, err := file.ReadFrom(f)
			return err
		}

		_, err = copyChunk(f, file, int64(chunkSize))
		if err != nil {
			return err
		}

		if err = file.Close(); err != nil {
			return err
		}
	}

	return nil
}

func splitFileParallel(ctx context.Context, count, goroutine, bufferMB int, filename, filenamePrefix string) error {
	fi, err := os.Stat(filename)
	if err != nil {
		return err
	}
	fileSize := fi.Size()

	// see logic in split function
	linesPerChunk := int((fileSize / int64(linelength)) / int64(count))
	chunkSize := linesPerChunk * linelength
	errs, _ := errgroup.WithContext(ctx)
	errs.SetLimit(goroutine)

	for i := 0; i < count; i++ {
		i := i
		errs.Go(func() error {
			source, err := os.Open(filename)
			if err != nil {
				return err
			}
			destination, err := os.OpenFile(fmt.Sprintf("%s_%04d.txt", filenamePrefix, i+1), os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return nil
			}
			// TODO use bufio to take "bufferMB" into account
			_, err = source.Seek(int64(chunkSize*i), io.SeekStart)
			if err != nil {
				return err
			}

			if i == count-1 {
				// Write everything we have left
				// The last file may be larger than the previous chunks!
				_, err := destination.ReadFrom(source)
				return err
			}

			_, err = copyChunk(source, destination, int64(chunkSize))
			if err != nil {
				return err
			}

			source.Close()
			return destination.Close()
		})
	}
	return errs.Wait()
}

func duration(msg string, start time.Time) {
	fmt.Printf("%s took %s\n", msg, time.Since(start))
}

func main() {
	flag.Parse()

	if version {
		fmt.Println(ver)
		os.Exit(0)
	}

	if count == 0 {
		fmt.Fprintln(os.Stderr, "count is required")
		flag.Usage()
		os.Exit(1)
	}

	if goroutine > count {
		goroutine = count
	}

	filenamePrefix := strings.Split(filename, ".")[0]

	defer duration("split", time.Now())

	var err error
	if parallel {
		err = splitFileParallel(context.Background(), count, goroutine, buffer, filename, filenamePrefix)
	} else {
		err = splitFile(count, buffer, filename, filenamePrefix)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
