CC ?= gcc
CFLAGS ?= -O2
CFLAGS += -std=gnu11 -Wall -Wextra -Werror -pthread
PYTHON ?= python3

all: build/siano-tsbuf

build/siano-tsbuf: src/siano-tsbuf.c
	mkdir -p build
	$(CC) $(CFLAGS) -o $@ $<

test: build/siano-tsbuf
	cd tests && TSBUF_BINARY=../build/siano-tsbuf $(PYTHON) -m unittest -v test_tsbuf

clean:
	rm -rf build

.PHONY: all test clean
