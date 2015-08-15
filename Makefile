#  -*-Mode: makefile;-*-	Requires GNU Make.
# Copyright (c) 2005-2010, J. Greg Davidson.
# You may use this file under the terms of the
# GNU AFFERO GENERAL PUBLIC LICENSE 3.0
# as specified in the file LICENSE.md included with this distribution.
# All other use requires my permission in writing.

# spx can be built without refs and for convenience they are now
# all in one directory and linked into the same shared object.
include ../Makefile.wicci
include Makefile.refs
include Makefile-spx
include Makefile-refs
DEBUG_H=debug.h debug-log.h
DEBUG=$(DEBUG_H)
all: debug-test-run refs-sizes.sql spx.so $(DepMakes) $(SchemaOut)
$M/debug-test: debug-test.o debug-log.o
	cc -o $@ $^
debug-test-run: $M/debug-test
	rm -f debug-test.log
	$M/debug-test
	diff debug-test.log debug-test.log.good
$M/debug-log.o: debug-log.c $(DEBUG)
	$(CC)  $(CFLAGS) -c -o $@ $<
$M/debug-test.o: debug-test.c $(DEBUG)
	$(CC)  $(CFLAGS) -c -o $@ $<
$M/spx.so: spx.o refs.o debug-log.o
	rm -f $@
	$(CC) $(CFLAGS) -shared -o $@  $M/spx.o $M/refs.o $M/debug-log.o
#	$(CC) $(CFLAGS) -shared -o $@ $^
-include $(DepMakes)
