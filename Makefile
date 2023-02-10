BINARY := filesplit

clean:
	rm -f input_*.txt

build: clean
	rm -f $(BINARY)
	CGO_ENABLED=0 go build -ldflags="-s -w" .

run: build
	./$(BINARY) -h
