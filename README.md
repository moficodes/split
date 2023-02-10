# Split

This project is for separating a large file into smaller chunks.

## Usage

```bash
go build
./filesplit -h
```

Should show you the help message.

```bash
Usage of ./filesplit:
  -buffer int
        buffer size in MB (default 1)
  -count int
        split the file in these many files
  -filename string
        file name to split (default "input.txt")
  -parallel
        split the file in parallel (default false)
  -version
        show version
```

## Example

```bash
# will generate 1000000 numbers in input.txt with 1Mb buffer and 5 goroutine
./generate -count 1000000 -file input.txt -buffer 1 -goroutine 5
```
