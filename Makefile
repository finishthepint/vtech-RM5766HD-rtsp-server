TOOLCHAIN_BIN ?= $(abspath ../vtech/toolchain/mips32el--uclibc--stable-2020.08-1/bin)
CROSS_COMPILE ?= $(if $(wildcard $(TOOLCHAIN_BIN)/mipsel-linux-gcc),$(TOOLCHAIN_BIN)/mipsel-linux-,mipsel-linux-)

CC := $(CROSS_COMPILE)gcc
CFLAGS := -O2 -Wall

all: bin/librtsp_cam.so bin/telnet_btn

bin/librtsp_cam.so: src/librtsp_cam.c
	@mkdir -p bin
	$(CC) $(CFLAGS) -fPIC -shared $< -o $@

bin/telnet_btn: src/telnet_btn.c
	@mkdir -p bin
	$(CC) $(CFLAGS) $< -o $@

test: src/telnet_btn.c
	gcc -O2 -Wall -Wextra -DUNIT_TEST $< -o /tmp/telnet_btn_test
	/tmp/telnet_btn_test
	@rm -f /tmp/telnet_btn_test

clean:
	rm -f /tmp/telnet_btn_test

.PHONY: all test clean
