package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	filename  string
	count     int
	goroutine int
	version   bool
	ver       string
	buffer    int
	parallel  bool
	VERSION   string = "v0.0.0"
)

func init() {
	flag.StringVar(&filename, "filename", "input.txt", "file name to split")
	flag.IntVar(&count, "count", 0, "split the file in these many files")
	flag.BoolVar(&version, "version", false, "show version")
	flag.IntVar(&buffer, "buffer", 1, "buffer size in MB")
	flag.BoolVar(&parallel, "parallel", false, "split the file in parallel (default false)")
	flag.Parse()
}

func split(count int, filename, filenamePrefix string) error {
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
	chunkSize := fileSize / int64(count)

	for i := 0; i < count; i++ {
		f.Seek(chunkSize*int64(i), 0)
		buf := make([]byte, chunkSize)
		file, err := os.OpenFile(fmt.Sprintf("%s_%d.txt", filenamePrefix, i+1), os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		f.Read(buf)
		file.Write(buf)
		file.Close()
	}

	return nil
}

func splitParallel(count, goroutine, buffer int, filename, filenamePrefix string, ctx context.Context) error {
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
	chunkSize := fileSize / int64(count)
	bufferSize := buffer * 1024 * 1024

	errs, _ := errgroup.WithContext(ctx)

	for i := 0; i < count; i++ {
		i := i
		errs.Go(func() error {
			source, err := os.OpenFile(filename, os.O_RDONLY, 0644)
			if err != nil {
				return err
			}
			destination, err := os.OpenFile(fmt.Sprintf("%s_%d.txt", filenamePrefix, i+1), os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return nil
			}
			source.Seek(chunkSize*int64(i), 0)
			readSoFar := 0
			buf := make([]byte, bufferSize)
			for readSoFar < int(chunkSize) {
				n, err := source.Read(buf)
				if err != nil {
					return err
				}
				destination.Write(buf[:n])
				readSoFar += n
				buf = make([]byte, bufferSize)
			}
			source.Close()
			destination.Close()
			return nil

		})
	}
	return errs.Wait()
}

func duration(msg string, start time.Time) {
	fmt.Printf("%s took %s\n", msg, time.Since(start))
}

func main() {
	if version {
		fmt.Println(ver)
		os.Exit(0)
	}

	if goroutine > count {
		goroutine = count
	}

	if count == 0 {
		fmt.Println("count is required")
		os.Exit(1)
	}

	if goroutine == 0 {
		goroutine = runtime.GOMAXPROCS(-1)
	}

	filenamePrefix := strings.Split(filename, ".")[0]

	defer duration("split", time.Now())

	if parallel {
		err := splitParallel(count, goroutine, buffer, filename, filenamePrefix, context.Background())
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	err := split(count, filename, filenamePrefix)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
