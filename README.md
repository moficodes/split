# Split

This project is for separating a large file into smaller chunks.

## Generate data

Filesplit needs a file to split. You get the generate program to generate a file with random numbers.

You can find the latest release from [here](https://github.com/moficodes/generate/releases)

### Linux

64 bit

```bash
wget https://github.com/moficodes/generate/releases/download/v0.0.22/generate_Linux_x86_64.tar.gz
```

### Mac OS

For apple silicon

```bash
wget https://github.com/moficodes/generate/releases/download/v0.0.22/generate_Darwin_arm64.tar.gz
```

For intel

```bash
wget https://github.com/moficodes/generate/releases/download/v0.0.22/generate_Darwin_x86_64.tar.gz
```



Extract the tar file 

```bash
tar -xvf generate_YOUR_OS_YOUR_ARCH.tar.gz
```

Move the binary to your path

```bash
mv generate /usr/local/bin
```

Then you can generate data.

```bash
generate -count 100000 
```

This will generate a file with 100000 random numbers.

## Usage

```bash
make run
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
./filesplit -count 100 -file input.txt -buffer 1
```
